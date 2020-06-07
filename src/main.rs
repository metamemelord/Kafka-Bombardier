use clap::{clap_app, crate_version};
use rayon::prelude::*;
use rdkafka::config::FromClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};
use rdkafka::ClientConfig;
use std::fs;
use std::fs::File;
use std::io::{stdin, stdout, BufRead, BufReader, Write};
use std::path::PathBuf;

fn worker(brokers: String, f: File, topic_name: String) {
    let reader = BufReader::new(f);
    reader
        .lines()
        .map(|line| line.unwrap())
        .par_bridge()
        .for_each_init(
            || build_producer(brokers.clone()),
            move |producer, line| match producer
                .send(BaseRecord::to(&topic_name).payload(&line).key(""))
            {
                Ok(_) => (),
                Err(e) => println!("Error while producing message {:?}", e),
            },
        );
    println!("Done");
}

fn build_producer(brokers: String) -> BaseProducer {
    BaseProducer::from_config(
        ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("message.timeout.ms", "5000"),
    )
    .expect("Producer creation error")
}

fn main() {
    let app = clap_app!(
      kafka_bombardier =>
      (version: crate_version!())
      (about: "Bombard your kafka broker with messages from multiple threads!")
      (author: "Gaurav Saini")
      (@arg TOPIC: -t --topic +required +takes_value "Topic to send messages to")
      (@arg THREADS: -T --threads +takes_value "Number of threads to use")
      (@arg INPUT_FILE: -f --file +takes_value "Input file")
      (@arg KAFKA_BROKERS: -b --brokers +required +takes_value "Brokers list seperated by commas")
    )
    .get_matches();

    let brokers = app.value_of("KAFKA_BROKERS").unwrap().to_string();
    let topic = app.value_of("TOPIC").unwrap();
    let num_threads: usize = match app.value_of("THREADS") {
        Some(val) => val.parse::<usize>().expect("Invalid number of threads"),
        None => 1,
    };

    println!(
        "Kafka Bombardier ({})\nKafka Host(s): {}",
        crate_version!(),
        brokers,
    );

    if app.is_present("INPUT_FILE") {
        println!("\nRunning in file mode. Use Ctrl+c to exit.\n");
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
            .unwrap();

        let absolute_file_path =
            fs::canonicalize(&PathBuf::from(app.value_of("INPUT_FILE").unwrap()))
                .expect("Cannot retrieve absolute path of input file");
        let file_handler = File::open(absolute_file_path).expect("Error while creating the file");
        worker(brokers, file_handler, String::from(topic));
    } else {
        println!("\nRunning in interactive mode. Use Ctrl+c to exit.\n");
        let producer = build_producer(brokers);
        loop {
            let mut buf = String::new();
            print!("[{}]> ", topic);
            let _ = stdout().flush().unwrap();
            let _ = stdin()
                .read_line(&mut buf)
                .expect("Error while reading the line");
            buf = buf.trim_end().to_string();
            match producer.send(BaseRecord::to(&topic).payload(&buf).key("")) {
                Ok(_) => (),
                Err(e) => println!("Error while producing message {:?}", e),
            };
        }
    }
}
