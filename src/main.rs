use chrono::{Datelike, NaiveDate, Utc};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;
use std::{
    fs::OpenOptions,
    io::Write,
    sync::{Arc, Mutex},
};

use statrs::distribution::{Continuous, ContinuousCDF, Normal};
use std::f64::consts::E;
// use futures::stream::Collect;
use mongodb::{
    bson::{doc, Bson, Document},
    error::Result,
    options::FindOptions,
    results,
    sync::{Client, Collection, Cursor},
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

static GLOBAL_COUNT: AtomicUsize = AtomicUsize::new(0);
static MISSING_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, Serialize)]
enum DocValue {
    String(String),
    Float(f64),
    Integer(i32),
}

fn to_bson_value(value: &DocValue) -> Bson {
    match value {
        DocValue::String(v) => Bson::String(v.clone()),
        DocValue::Float(v) => Bson::Double(*v),
        DocValue::Integer(v) => Bson::Int32(*v),
    }
}

fn convert_to_bson_document(doc: &HashMap<&str, DocValue>) -> Document {
    let mut bson_doc = Document::new();

    for (key, value) in doc {
        bson_doc.insert(key.to_string(), to_bson_value(value));
    }

    bson_doc
}

// Define your document structure here
#[derive(Debug, Serialize, Deserialize)]
struct SpotDoc {
    minute: String,
    close: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct GroupedSpotDoc {
    date: String,
    documents: Vec<SpotDoc>,
}

fn list_filtered_collections_by_regex(
    db: &mongodb::sync::Database,
    regex_pattern: &str,
) -> Vec<String> {
    let filter_query = doc! {
        "name": {
            "$regex": regex_pattern
        }
    };
    db.list_collection_names(Some(filter_query)).unwrap()
}

fn black_scholes_call_price(
    spot: f64,
    strike: f64,
    time_to_expiry: f64,
    interest_rate: f64,
    volatility: f64,
    dividend: f64,
) -> f64 {
    let d1 = (f64::ln(spot / strike)
        + (interest_rate - dividend + 0.5 * volatility.powi(2)) * time_to_expiry)
        / (volatility * f64::sqrt(time_to_expiry));
    let d2 = d1 - volatility * f64::sqrt(time_to_expiry);
    let norm = Normal::new(0.0, 1.0).unwrap();
    let call_price = E.powf(-dividend * time_to_expiry) * spot * norm.cdf(d1)
        - E.powf(-interest_rate * time_to_expiry) * strike * norm.cdf(d2);
    call_price
}

fn black_scholes_put_price(
    spot: f64,
    strike: f64,
    time_to_expiry: f64,
    interest_rate: f64,
    volatility: f64,
    dividend: f64,
) -> f64 {
    let d1 = (f64::ln(spot / strike)
        + (interest_rate - dividend + 0.5 * volatility.powi(2)) * time_to_expiry)
        / (volatility * f64::sqrt(time_to_expiry));
    let d2 = d1 - volatility * f64::sqrt(time_to_expiry);
    let norm = Normal::new(0.0, 1.0).unwrap();
    let put_price = E.powf(-interest_rate * time_to_expiry) * strike * norm.cdf(-d2)
        - E.powf(-dividend * time_to_expiry) * spot * norm.cdf(-d1);
    put_price
}

fn implied_volatility<'a>(
    option_price: f64,
    spot: f64,
    strike: f64,
    time_to_expiry: f64,
    interest_rate: f64,
    // minute: &'a str,
    // instrument: &'a str,
    option_type: &'a str,
    dividend: f64,
    tolerance: f64,
    max_iterations: usize,
) -> (HashMap<&'a str, DocValue>, f64) {
    let mut volatility = 0.2;
    let normal = Normal::new(0.0, 1.0).unwrap();
    let mut r1: HashMap<&str, DocValue> = HashMap::from([]);
    for _ in 0..max_iterations {
        let (price, vega) = match option_type {
            "call" => {
                let price = black_scholes_call_price(
                    spot,
                    strike,
                    time_to_expiry,
                    interest_rate,
                    volatility,
                    dividend,
                );
                let d1 = (f64::ln(spot / strike)
                    + (interest_rate - dividend + 0.5 * volatility.powi(2)) * time_to_expiry)
                    / (volatility * time_to_expiry.sqrt());
                let vega = spot
                    * E.powf(-dividend * time_to_expiry)
                    * normal.pdf(d1)
                    * time_to_expiry.sqrt();
                (price, vega)
            }
            "put" => {
                let price = black_scholes_put_price(
                    spot,
                    strike,
                    time_to_expiry,
                    interest_rate,
                    volatility,
                    dividend,
                );
                let d1 = (f64::ln(spot / strike)
                    + (interest_rate - dividend + 0.5 * volatility.powi(2)) * time_to_expiry)
                    / (volatility * time_to_expiry.sqrt());
                let vega = spot
                    * E.powf(-dividend * time_to_expiry)
                    * normal.pdf(d1)
                    * time_to_expiry.sqrt();
                (price, vega)
            }
            _ => panic!("Invalid option type. Use 'call' or 'put'."),
        };

        let price_diff = price - option_price;
        // If the difference is within tolerance, return the result
        if price_diff.abs() < tolerance {
            let iv = (&volatility * 10_00000.0).round() / 10_000.0;
            r1 = HashMap::from([
                // ("minute", DocValue::String(minute.to_string())),
                // ("ticker", DocValue::String(instrument.to_string())),
                ("strike", DocValue::Float(strike)),
                ("close", DocValue::Float(option_price)),
                ("spot_price", DocValue::Float(spot)),
                // ("date", DocValue::String(minute.split(' ').next().unwrap_or("").to_string())),
                // ("time", DocValue::String(minute.split(' ').nth(1).unwrap_or("").to_string())),
                ("iv", DocValue::Float(iv)),
                (
                    "ce_pe",
                    DocValue::String(if option_type == "call" {
                        "CE".to_string()
                    } else {
                        "PE".to_string()
                    }),
                ),
            ]);
            return (r1, volatility);
        }

        // Update volatility using Newton-Raphson method
        volatility -= price_diff / vega;
    }
    // println!("optioPrice:{:?}, spot:{:?}, strike:{:?}, time_to_expiry:{:?}",option_price,spot,strike,time_to_expiry);
    // println!("{:?} -->  {:?}",r1,volatility);
    // If not converged, return zero volatility
    (r1, 0.0)
}

fn calculate_greeks(
    spot: f64,
    strike: f64,
    time_to_expiry: f64,
    interest_rate: f64,
    volatility: f64,
    option_type: &str,
    dividend: f64,
) -> HashMap<&str, f64> {
    let normal = Normal::new(0.0, 1.0).unwrap();
    let d1 = (f64::ln(spot / strike)
        + (interest_rate - dividend + 0.5 * volatility.powi(2)) * time_to_expiry)
        / (volatility * time_to_expiry.sqrt());
    let d2 = d1 - volatility * time_to_expiry.sqrt();

    let delta = if option_type == "call" {
        E.powf(-dividend * time_to_expiry) * normal.cdf(d1)
    } else {
        E.powf(-dividend * time_to_expiry) * (normal.cdf(d1) - 1.0)
    };

    let gamma = E.powf(-dividend * time_to_expiry) * normal.pdf(d1)
        / (spot * volatility * time_to_expiry.sqrt());

    let vega =
        spot * E.powf(-dividend * time_to_expiry) * normal.pdf(d1) * time_to_expiry.sqrt() / 100.0;

    let theta = if option_type == "call" {
        (-E.powf(-dividend * time_to_expiry) * (spot * normal.pdf(d1) * volatility)
            / (2.0 * time_to_expiry.sqrt())
            - interest_rate * strike * E.powf(-interest_rate * time_to_expiry) * normal.cdf(d2)
            + dividend * spot * E.powf(-dividend * time_to_expiry) * normal.cdf(d1))
            / 365.25
    } else {
        (-E.powf(-dividend * time_to_expiry) * (spot * normal.pdf(d1) * volatility)
            / (2.0 * time_to_expiry.sqrt())
            + interest_rate * strike * E.powf(-interest_rate * time_to_expiry) * normal.cdf(-d2)
            - dividend * spot * E.powf(-dividend * time_to_expiry) * normal.cdf(-d1))
            / 365.25
    };

    let r2: HashMap<&str, f64> = HashMap::from([
        ("delta", (delta * 10_000.0).round() / 10_000.0),
        ("gamma", (gamma * 10_000.0).round() / 10_000.0),
        ("vega", (vega * 10_000.0).round() / 10_000.0),
        ("theta", (theta * 10_000.0).round() / 10_000.0),
    ]);

    r2
}

fn calculate_implied_volatilities_and_greeks<'a>(
    spot: f64,
    strike: f64,
    call_price: Option<f64>,
    put_price: Option<f64>,
    current_date: chrono::NaiveDate,
    expiry_date: chrono::NaiveDate,
    // minute: &'a str,
    // instrument: &'a str,
    interest_rate: f64,
    dividend: f64,
) -> HashMap<&'a str, DocValue> {
    let time_to_expiry = (expiry_date - current_date).num_days() as f64 / 365.25;

    // let greeks;
    // let mut IV:HashMap<&str,DocValue>;
    if let Some(call_price) = call_price {
        let (iv, volatility) = implied_volatility(
            call_price,
            spot,
            strike,
            time_to_expiry,
            interest_rate,
            "call",
            dividend,
            1e-6,
            100,
        );
        if volatility != 0.0 {
            let mut row: HashMap<&str, DocValue> = HashMap::from([]);
            let greeks = calculate_greeks(
                spot,
                strike,
                time_to_expiry,
                interest_rate,
                volatility,
                "call",
                dividend,
            );
            if !greeks.is_empty() {
                row = iv.clone();
                for (k, v) in greeks {
                    row.insert(k.clone(), DocValue::Float(v.clone()));
                }
                return row;
            }
        }
    }

    if let Some(put_price) = put_price {
        let (iv, volatility) = implied_volatility(
            put_price,
            spot,
            strike,
            time_to_expiry,
            interest_rate,
            "put",
            dividend,
            1e-6,
            100,
        );
        if volatility != 0.0 {
            let mut row: HashMap<&str, DocValue> = HashMap::from([]);
            let greeks = calculate_greeks(
                spot,
                strike,
                time_to_expiry,
                interest_rate,
                volatility,
                "put",
                dividend,
            );
            if !greeks.is_empty() {
                row = iv.clone();
                for (k, v) in greeks {
                    row.insert(k.clone(), DocValue::Float(v.clone()));
                }
                return row;
            }
        }
    }
    HashMap::from([])
}

fn fetch_expiry_data(main_db: &mongodb::sync::Database) -> HashMap<String, Document> {
    let mut expiry_data: HashMap<String, Document> = HashMap::new();
    let current_year = Utc::now().year();

    let historical_collections: Vec<_> = (2017..=current_year)
        .map(|year| main_db.collection::<Document>(&format!("option_historical_{}", year)))
        .collect();

    for collection in historical_collections {
        let pipeline = vec![doc! {
            "$project": {
                "ce_pe": 1,
                "to_expiry": 1,
                "option_name": 1,
                "option": 1,
                "strike_price": 1,
                "_id": 0
            }
        }];

        // Specify the correct type for Cursor<Document>
        let cursor = collection.aggregate(pipeline, None).unwrap();
        for result in cursor {
            if let Ok(doc) = result {
                if let Some(option) = doc.get_str("option").ok() {
                    let key = option.split(':').nth(1).unwrap_or_default().to_string();
                    expiry_data.insert(key, doc);
                }
            }
        }
    }

    expiry_data
}

fn fetch_and_process_spot_docs(
    collection: &Collection<Document>,
    option_collection: &Collection<Document>,
    first_doc: &str,
    expiries: &HashMap<String, Document>,
    greeks_db: &mongodb::sync::Database,
    big_db: &mongodb::sync::Database,
    num_threads: usize,
) {
    let pipeline = vec![
        doc! {"$match": {"minute": {"$gte": first_doc}}},
        doc! {
            "$group": {
                "_id": "$date",
                "documents": {
                    "$push": {
                        "minute": "$minute",
                        "close": "$close"
                    }
                }
            }
        },
        doc! {"$sort": {"_id": 1}},
    ];

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("missing.txt")
        .expect("Unable to open error.txt");
    writeln!(file, "Running Script for : {}", option_collection.name())
        .expect("Unable to write to error.txt");

    GLOBAL_COUNT.store(0, Ordering::SeqCst);

    let aggregate_options = mongodb::options::AggregateOptions::builder()
        .allow_disk_use(true)
        .build();
    let cursor = collection
        .aggregate(pipeline, Some(aggregate_options))
        .expect("Failed to aggregate spot collection");

    let docs: Vec<Document> = cursor
        .map(|result| result.expect("Failed to parse document"))
        .collect();

    docs.par_iter().with_max_len(num_threads).for_each(|group| {
        process_group_by_date(group, option_collection, &greeks_db, &big_db, &expiries)
    });
}

fn process_group_by_date(
    group: &Document,
    options_collection: &Collection<Document>,
    greeks_db: &mongodb::sync::Database,
    big_db: &&mongodb::sync::Database,
    expiry_data: &HashMap<String, Document>,
) {
    // Extract the date and documents array from the group document
    let date = group.get_str("_id").unwrap_or_default();
    let documents = group.get_array("documents").unwrap();
    // let lock: Arc<Mutex<()>>;
    let mut bulk_operations: Vec<Document> = vec![]; // Changed to store `Document` objects for `insert_many`
    let mut greeks_collection_name = String::from("");

    // Collect all "minute" and "close" values
    let mins: Vec<String> = documents
        .iter()
        .map(|doc| {
            doc.as_document()
                .unwrap()
                .get_str("minute")
                .unwrap()
                .to_string()
        })
        .collect();

    let close_price: HashMap<String, f64> = documents
        .iter()
        .filter_map(|doc| {
            let doc = doc.as_document().unwrap();
            let minute = doc.get_str("minute").unwrap().to_string();
            let close = doc.get_f64("close").ok();
            close.map(|c| (minute, c))
        })
        .collect();

    let mut options_cursor: Vec<Document> = vec![];

    // Lock to prevent concurrent access when fetching documents
    {
        // let _lock_guard = lock.lock().unwrap();
        for mins_slice in mins.chunks(100) {
            let cursor: Cursor<Document> = options_collection
                .aggregate(
                    vec![
                        doc! { "$match": { "minute": { "$in": mins_slice } } },
                        doc! { "$project": { "minute": 1, "close": 1, "ticker": 1, "date": 1, "_id": 0 } },
                    ],
                    None,
                )
                .expect("Failed to aggregate options");
            options_cursor.extend(cursor.map(|doc| doc.unwrap()));
        }
    }

    // println!("Processing date: {}, length: {}, options: {}, Name: {}", date, documents.len(), options_cursor.len(), options_collection.name());

    if options_cursor.len() == 0 {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("missing.txt")
            .expect("Unable to open error.txt");
        writeln!(file, "Missing Date: {}", date).expect("Unable to write to error.txt");
    }

    // Iterate through options documents
    for option_doc in options_cursor {
        let ticker = option_doc.get_str("ticker").unwrap();
        let option_name = ticker;
        let instrument = ticker.to_string();
        let spot_price = close_price[option_doc.get_str("minute").unwrap()];
        let minute = option_doc.get_str("minute").unwrap().to_string();

        // Fetch the corresponding expiry document
        let expiry_doc = match expiry_data.get(option_name) {
            Some(expiry_doc) => expiry_doc,
            None => {
                // Log error if expiry data is not found
                println!("Expiry data not found for ticker: {}", ticker);
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("error.txt")
                    .expect("Unable to open error.txt");
                writeln!(file, "Expiry data not found for ticker: {}", ticker)
                    .expect("Unable to write to error.txt");
                continue;
            }
        };
        // println!("Expiry Doc --> {:?}", expiry_doc);
        let strike_price = match expiry_doc.get_i32("strike_price") {
            Ok(price) => price as f64, // If it's i32, convert to f64
            Err(_) => expiry_doc.get_f64("strike_price").unwrap_or_else(|_| {
                println!("Unable to retrieve 'strike_price' as i32 or f64");
                0.0
            }), // If it's not i32, try f64, or handle the error
        };
        if strike_price == 0.0 {
            continue;
        }
        greeks_collection_name =
            (String::from("op_") + expiry_doc.get_str("option_name").unwrap()).to_lowercase();
        let expiry_date =
            NaiveDate::parse_from_str(expiry_doc.get_str("to_expiry").unwrap(), "%d-%b-%y")
                .expect("Failed to parse expiry date");
        let current_date =
            NaiveDate::parse_from_str(option_doc.get_str("date").unwrap(), "%Y-%m-%d")
                .expect("Failed to parse current date");

        let call_price = if expiry_doc.get_str("ce_pe").unwrap() == "CE" {
            Some(option_doc.get_f64("close").unwrap())
        } else {
            None
        };

        let put_price = if expiry_doc.get_str("ce_pe").unwrap() == "PE" {
            Some(option_doc.get_f64("close").unwrap())
        } else {
            None
        };

        // Calculate Greeks and implied volatility
        let mut row: HashMap<&str, DocValue> = calculate_implied_volatilities_and_greeks(
            spot_price,
            strike_price as f64,
            call_price,
            put_price,
            current_date,
            expiry_date,
            0.1,
            0.0,
        );

        // If the row is valid, add it to bulk operations
        if !row.is_empty() {
            row.insert("minute", DocValue::String(minute.to_string()));
            row.insert(
                "date",
                DocValue::String(minute.split(' ').next().unwrap_or("").to_string()),
            );
            row.insert(
                "time",
                DocValue::String(minute.split(' ').nth(1).unwrap_or("").to_string()),
            );
            row.insert("ticker", DocValue::String(instrument.to_string()));
            GLOBAL_COUNT.fetch_add(1, Ordering::SeqCst);
            bulk_operations.push(convert_to_bson_document(&row));
        } else {
            MISSING_COUNT.fetch_add(1, Ordering::SeqCst);
        }

        if bulk_operations.len() >= 60000 {
            let greeks_collection = greeks_db.collection(&greeks_collection_name);

            // Check if the collection exists, and create it if it doesn't
            match big_db.list_collection_names(None) {
                Ok(collections) => {
                    if !collections.contains(&greeks_collection_name) {
                        if let Err(e) = big_db.create_collection(&greeks_collection_name, None) {
                            eprintln!("Failed to create collection: {:?}", e);
                            return;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to list collection names: {:?}", e);
                    return;
                }
            }

            // Attempt to insert the documents
            if let Err(e) = greeks_collection.insert_many(bulk_operations, None) {
                eprintln!("Failed to insert documents: {:?}", e);
                return;
            }
            bulk_operations = vec![];
            thread::sleep(Duration::from_secs(1));
        }
    }

    // Perform the insert_many operation to MongoDB
    if !bulk_operations.is_empty() || !greeks_collection_name.is_empty() {
        let greeks_collection = greeks_db.collection(&greeks_collection_name);

        // Check if the collection exists, and create it if it doesn't
        match big_db.list_collection_names(None) {
            Ok(collections) => {
                if !collections.contains(&greeks_collection_name) {
                    if let Err(e) = big_db.create_collection(&greeks_collection_name, None) {
                        eprintln!("Failed to create collection: {:?}", e);
                        return;
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to list collection names: {:?}", e);
                return;
            }
        }

        // Attempt to insert the documents
        if let Err(e) = greeks_collection.insert_many(bulk_operations, None) {
            eprintln!("Failed to insert documents: {:?}", e);
            return;
        }
        thread::sleep(Duration::from_secs(1));
        // Print the count of inserted rows
    }
    println!(
        "Total Inserted rows: {:?}, Missing Count: {:?}, Collection: {:?}",
        GLOBAL_COUNT.load(Ordering::SeqCst),
        MISSING_COUNT.load(Ordering::SeqCst),
        greeks_collection_name
    );
}

fn process_collections(
    spot_collections: Vec<String>,
    option_collections: Vec<String>,
    big_db: &mongodb::sync::Database,
    greeks_db: &mongodb::sync::Database,
    expiries: HashMap<String, Document>,
) {
    // Index Mapper
    let index_mapper: HashMap<&str, &str> = HashMap::from([
        ("in_nsei_1min", "op_nifty"),
        ("in_nsebank_1min", "op_banknifty"),
        ("in_midcpnifty_1min", "op_midcpnifty"),
        ("in_niftyfinance_1min", "op_finnifty"),
    ]);

    for spot_coll in spot_collections {
        let instrument_name: String;
        let option_collection: mongodb::sync::Collection<Document>;

        if spot_coll.starts_with("eq_") {
            instrument_name = format!("op_{}", spot_coll.replace("eq_", "").replace("_1min", ""));
            option_collection = big_db.collection(&instrument_name);
        } else if spot_coll.starts_with("in_") {
            if let Some(&mapped_name) = index_mapper.get(spot_coll.as_str()) {
                instrument_name = mapped_name.to_string();
                option_collection = big_db.collection(mapped_name);
            } else {
                continue;
            }
        } else {
            continue;
        }

        // println!("Checking Collection {}",instrument_name);
        if !option_collections.contains(&instrument_name)
        {
            continue;
        }

        let collection = big_db.collection(&spot_coll);
        let find_options = FindOptions::builder()
            .sort(doc! {"minute":1})
            .limit(1)
            .build();

        let first_doc: Vec<Document> = option_collection
            .find(None, find_options)
            .unwrap()
            .map(|result| result.unwrap())
            .collect();

        if let Some(minute_value) = first_doc[0].get("minute") {
            // println!(
            //     "Processing collection: {}, minute: {:?}",
            //     spot_coll, minute_value
            // );
            fetch_and_process_spot_docs(
                &collection,
                &option_collection,
                minute_value.as_str().unwrap(),
                &expiries,
                &greeks_db,
                &big_db,
                5,
            );
        }
    }
}

fn main() -> Result<()> {
    // Connection string for big_client
    let big_uri = "mongodb://Unfluke:Temp111!@43.204.111.72:27017/Unfluke?authSource=admin&readPreference=primary&directConnection=true&ssl=false";
    let main_uri = "mongodb://Unfluke:Temp111!@3.6.244.31:27017/Unfluke?authSource=admin&replicaSet=rs0&readPreference=primary&directConnection=true&ssl=false";

    // Create clients synchronously
    let big_client = Client::with_uri_str(big_uri)?;
    let main_client = Client::with_uri_str(main_uri)?;

    println!("Connected to MongoDB");

    // Access the databases
    let big_db = big_client.database("Unfluke");
    let greeks_db = big_client.database("Greeks");
    let main_db = main_client.database("Unfluke");

    let spot_collections = big_db
        .list_collection_names(Some(doc! {
            "name": {
                "$regex": r"^(eq_).*_1min$"
            }
        }))
        .unwrap();

    let option_collections = list_filtered_collections_by_regex(&big_db, r"^op_");

    // let spot_collections= vec![String::from("in_nsei_1min")];
    // let option_collections = vec![String::from("op_nifty")];

    println!("Fetching Expiries ...");
    let expiries = fetch_expiry_data(&main_db);
    println!("Expiry Fetched");
    process_collections(
        spot_collections,
        option_collections,
        &big_db,
        &greeks_db,
        expiries,
    );

    Ok(())
}
