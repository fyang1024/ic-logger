import Buffer "mo:base/Buffer";
import Cycles "mo:base/ExperimentalCycles"; 
import List "mo:base/List";
import Nat "mo:base/Nat";
import Logger "mo:ic-logger/Logger";

import TextLogger "./TextLogger";

actor class ScalableTextLogger() {

  private var msg_count: Nat = 0;
  private var loggers = Buffer.Buffer<TextLogger.TextLogger>(10);

  private let MAX_MSGS_PER_LOGGER = 100;
  private let CYCLE_LIMIT = 1_000_000_000_000;

  // Add an array of messages to the log.
  public func append(msgs: [Text]): async () {
    let msg_array_list = split(msgs);
    var i: Nat = 0;
    while (i < List.size(msg_array_list)) {
      switch (List.get(msg_array_list, i)) {
        case (null) {};
        case (?msg_array) { 
          let logger = await get_logger(msg_count / MAX_MSGS_PER_LOGGER);
          logger.append(msg_array);
          msg_count := msg_count + msg_array.size(); 
        }
      };
      i := i + 1;
    }
  };

  // Return the messages between from and to indice (inclusive).
  public func view(from: Nat, to: Nat) : async Logger.View<Text> {
    assert(to >= from);  
    let buf = Buffer.Buffer<Text>(to - from + 1);
    var start = from;
    var end = calc_end(start, to);
    label LOOP loop {
      switch (loggers.getOpt(start / MAX_MSGS_PER_LOGGER)) {
        case (null) { break LOOP; };
        case (?logger) {
          let v = await logger.view(start % MAX_MSGS_PER_LOGGER, end % MAX_MSGS_PER_LOGGER);
          for (msg in v.messages.vals()) {
            buf.add(msg);
          };
          start := end + 1;
          if (start > to) { break LOOP; };
          end := calc_end(start, to);
        } 
      }
    };
    {
      start_index = from;
      messages = buf.toArray()
    }
  };

  // make sure end is not out of bounds and there are max MAX_MSGS_PER_LOGGER messages between start and end inclusive   
  private func calc_end(start: Nat, to: Nat) : Nat {
    let max_end = start + MAX_MSGS_PER_LOGGER - 1 - start % MAX_MSGS_PER_LOGGER;  
    if (max_end > to) { to } else { max_end }
  };

  private func get_logger(index: Nat) : async TextLogger.TextLogger {
    switch (loggers.getOpt(index)) {
      case (null) {
        Cycles.add(CYCLE_LIMIT); // fund the new TextLogger canister
        let logger = await TextLogger.TextLogger();
        loggers.add(logger);
        logger
      };  
      case (?logger) { 
        logger 
      }
    }
  };

  //  split msgs into a list of msg arrays so that each array contains max MAX_MSGS_PER_LOGGER msgs.
  private func split(msgs: [Text]): List.List<[Text]> {
    var split_msgs: List.List<[Text]> = List.nil();
    var buf = Buffer.Buffer<Text>(MAX_MSGS_PER_LOGGER);
    var first_array = true;
    for (msg in msgs.vals()) {
      let full_size: Nat = if (first_array) {
        // first array may be appended to an existing logger
        // make sure the existing logger not to have more than MAX_MSGS_PER_LOGGER msgs
        MAX_MSGS_PER_LOGGER - msg_count % MAX_MSGS_PER_LOGGER
      } else { 
        MAX_MSGS_PER_LOGGER
      };
      buf.add(msg);  
      if (buf.size() == full_size) {
        split_msgs := List.push<[Text]>(buf.toArray(), split_msgs);
        buf := Buffer.Buffer<Text>(MAX_MSGS_PER_LOGGER);
        first_array := false;
      }
    };
    if (buf.size() > 0) {
      split_msgs := List.push<[Text]>(buf.toArray(), split_msgs);
    };
    // reverse it to maintain the order as the messages are pushed (pre-pended) one by one
    List.reverse(split_msgs)
  };
}
