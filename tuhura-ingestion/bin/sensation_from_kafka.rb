#! /usr/bin/env ruby
$: << File.join(File.dirname(__FILE__), '../lib')

require 'bundler/setup'
require 'tuhura/ingestion/sensation_from_kafka'

options = {task: :inject, max_msgs: -1}
Tuhura::Ingestion::SensationFromKafka.create(options).work(options)
