use std::{fs::File, collections::HashMap};
use std::io::prelude::*;

use serde_json::json;
use tokio::sync::mpsc::{self, Sender};

use reqwest::Client;
use anyhow::Result;
use serde::{Serialize, Deserialize};
use dotenv::dotenv;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv()?;

    let token = std::env::var("BEARER_TOKEN")?;
    let query = "(neubau OR tower OR baumarkt OR sanierung OR hochhaus OR mehrfamilienhaus OR hochhausbau OR möbelhaus OR showroom OR skyscraper OR mehrfamilienhäuser OR wolkenkratzer OR warenhaus OR baumesse OR küchenverkauf OR (bad studio) OR baugeschäft OR möbelhandel OR neubauviertel) lang:de -is:retweet";
    let fields = "author_id,created_at";
    let results = 4300;

    let (tx, mut rx) = mpsc::channel(128);

    tokio::spawn(async move {
        match search_tweets(tx, query, results, fields, &token).await {
            Err(e) => eprintln!("{:?}", e),
            _ => eprintln!("Send the tweets out"),
        }
    });

    let file_path = format!("../complete/twitter_data.json");
    let to_file_path = format!("../text_only/twitter_data.json");

    File::create(&file_path)?;
    File::create(&to_file_path)?;

    let mut file = File::options()
        .append(true)
        .open(file_path)?;

    let mut to_file = File::options()
        .append(true)
        .open(to_file_path)?;

    while let Some(tweet) = rx.recv().await {
        writeln!(file, "{}", serde_json::to_string(&tweet)?)?;

        let text_only = json!({
            "text": tweet.text.as_ref(),
        });

        writeln!(to_file, "{}", serde_json::to_string(&text_only)?)?;
    }

    Ok(())
}

async fn search_tweets(tx: Sender<Tweet>, query: &str, results: u32, fields: &str, token: &str) -> Result<()> {
    let client = Client::new();
    let url = "https://api.twitter.com/2/tweets/search/all";

    let mut params = HashMap::new();
    params.insert("query", query.to_string());

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
            .expect("valid json");

        let tweets = response.data.unwrap();
        next_token = response.meta.unwrap().next_token;

        for tweet in tweets {
            tx.send(tweet)
                .await
                .expect("working rx");
        }
    }

    let remainder = results % 500;

    if remainder > 0 {
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

        for tweet in tweets {
            tx.send(tweet)
                .await
                .expect("working rx");
        }
    }

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Data<T> {
    data: Option<T>,
    meta: Option<Meta>,
}

#[derive(Serialize, Deserialize)]
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
