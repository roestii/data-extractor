use std::time::Duration;
use std::{fs::File, collections::HashMap};
use std::io::prelude::*;

use clap::Parser;
use serde_json::json;
use tokio::sync::mpsc::{self, Sender};

use reqwest::Client;
use anyhow::Result;
use serde::{Serialize, Deserialize};
use dotenv::dotenv;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    query: String,

    #[clap(short, long, default_value_t = String::from("twitter_data.jsonl"))]
    output_file: String,

    #[clap(short, long, default_value_t = String::from("2020-03-05T00:00:00Z"))]
    start_date: String,

    #[clap(short, long, default_value_t = String::from("2022-03-05T00:00:00Z"))]
    end_date: String,

    #[clap(short, long, default_value_t = 4000)]
    results: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv()?;

    let Args { query, output_file, start_date, end_date, results }  = Args::parse();

    let token = std::env::var("BEARER_TOKEN")?;
    //let _old_query = "(neubau OR tower OR baumarkt OR sanierung OR hochhaus OR mehrfamilienhaus OR hochhausbau OR möbelhaus OR showroom OR skyscraper OR mehrfamilienhäuser OR wolkenkratzer OR warenhaus OR baumesse OR küchenverkauf OR (bad studio) OR baugeschäft OR möbelhandel OR neubauviertel) lang:de -is:retweet";
    //let query = "(((küchen studio) OR hochhaus) (neu OR insolvenz OR eröffnung)) lang:de -is:retweet";
    let fields = "author_id,created_at";

    let file_path = format!("../complete/{}", output_file);
    let to_file_path = format!("../text_only/{}", output_file);
    File::create(&file_path)?;
    File::create(&to_file_path)?;

    search_tweets(&query, results, fields, &token, &start_date, &end_date, &output_file).await;
    Ok(())
}

fn append_to_file(content: Vec<Tweet>, output_file: &str) -> std::io::Result<()> {
    let file_path = format!("../complete/{}", output_file);
    let to_file_path = format!("../text_only/{}", output_file);

    let mut file = File::options()
        .append(true)
        .open(file_path)?;

    let mut to_file = File::options()
        .append(true)
        .open(to_file_path)?;

    for tweet in content {
        writeln!(file, "{}", serde_json::to_string(&tweet)?)?;

        let text_only = json!({
            "text": tweet.text.as_ref(),
        });

        writeln!(to_file, "{}", serde_json::to_string(&text_only)?)?;
    }

    Ok(())
}

async fn search_tweets(
    query: &str, 
    results: u32,
    fields: &str,
    token: &str,
    start_time: &str,
    end_time: &str,
    output_file: &str
) -> Result<()> {
    let client = Client::new();
    let url = "https://api.twitter.com/2/tweets/search/all";

    let mut params = HashMap::new();
    params.insert("query", query.to_string());
    params.insert("start_time", start_time.to_string());
    params.insert("end_time", end_time.to_string());

    if results > 500 {
        params.insert("max_results", 500.to_string());
    } else {
        params.insert("max_results", results.to_string());
    }

    params.insert("tweet.fields", fields.to_string());
    let mut next_token: Option<String> = None;

    for _ in 0..(results / 500) {
        if let Some(ref token) = next_token {
            match params.get_mut("next_token") {
                Some(value) => {
                    *value = token.to_string();
                }
                None => {
                    params.insert("next_token", token.to_string());
                }
            }
        } else {
            params.remove("next_token");
        }

        let response: Data<Vec<Tweet>> = client.get(url)
            .query(&params)
            .bearer_auth(token)
            .send()
            .await
            .expect("to get something")
            .json()
            .await
            .expect("Valid JSON");

        let tweets = response.data.unwrap();
        append_to_file(tweets, output_file)?;

        next_token = match response.meta.unwrap().next_token {
            Some(next_token) => Some(next_token),
            None => break,
        };


        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let remainder = results % 500;

    if remainder > 0 && !next_token.is_none() {
        let value = params.get_mut("max_results").unwrap();
        *value = remainder.to_string();

        let response: Data<Vec<Tweet>> = client.get(url)
            .query(&params)
            .bearer_auth(token)
            .send()
            .await
            .expect("to get something")
            .json()
            .await
            .expect("valid json");

        let tweets = response.data.unwrap();
        append_to_file(tweets, output_file)?;
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct Data<T> {
    data: Option<T>,
    meta: Option<Meta>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Meta {
    newest_id: Option<String>,
    oldest_id: Option<String>,
    next_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Tweet {
    id: Option<String>,
    author_id: Option<String>,
    text: Option<String>,
    created_at: Option<String>,
    edit_histroy_tweet_ids: Option<Vec<String>>,
}
