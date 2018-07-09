'use strict';

var libPath = process.env['kafka-cov'] ? '../lib-cov/' : '../lib/';
var Producer = require(libPath + 'producer');
var Offset = require(libPath + 'offset');
var Client = require(libPath + 'client');
var ConsumerGroup = require(libPath + 'consumerGroup');
const uuid = require('uuid');

var client, producer, offset;

var host = process.env['KAFKA_TEST_HOST'] || '';

describe('Offset', function () {
  before(function (done) {
    client = new Client(host);
    producer = new Producer(client);
    producer.on('ready', function () {
      producer.createTopics(['_exist_topic_3_test'], true, function (err) {
        done(err);
      });
    });

    offset = new Offset(client);
  });

  after(function (done) {
    producer.close(done);
  });

  describe('#fetch', function () {
    it('should return offset of the topics', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [ { topic: topic } ];
      offset.fetch(topics, function (err, data) {
        var offsets = data[topic][0];
        offsets.should.be.an.instanceOf(Array);
        offsets.length.should.equal(1);
        done(err);
      });
    });

    it('should return earliest offset of the topics', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [ { topic: topic, time: -2 } ];
      offset.fetch(topics, function (err, data) {
        var offsets = data[topic][0];
        offsets.should.be.an.instanceOf(Array);
        offsets.length.should.above(0);
        done(err);
      });
    });

    it('should return latest offset of the topics', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [ { topic: topic, time: -1 } ];
      offset.fetch(topics, function (err, data) {
        var offsets = data[topic][0];
        offsets.should.be.an.instanceOf(Array);
        offsets.length.should.above(0);
        done(err);
      });
    });

    it('should keeping calling fetch until offset is ready', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [ { topic: topic } ];
      offset.fetch(topics, done);
    });
  });

  describe('#commit', function () {
    it('should commit successfully', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [ { topic: topic, offset: 10 } ];
      offset.commit('_groupId_commit_test', topics, function (err, data) {
        data.should.be.ok;
        Object.keys(data)[0].should.equal(topic);
        done(err);
      });
    });

    it('should keep calling commit until offset is ready', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [ { topic: topic, offset: 10 } ];
      offset.commit('_groupId_commit_test', topics, done);
    });
  });

  describe('#fetchCommits', function () {
    it('should get last committed offset of the consumer group', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [ { topic: topic, offset: 10 } ];
      offset.fetchCommits('_groupId_commit_1_test', topics, function (err, data) {
        data.should.be.ok;
        Object.keys(data)[0].should.equal(topic);
        data[topic][0].should.equal(-1);
        done(err);
      });
    });

    it('should keep calling fetchCommits until offset is ready', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [ { topic: topic, offset: 10 } ];
      offset.fetchCommits('_groupId_commit_1_test', topics, done);
    });
  });

  describe('#fetchCommitsV1', function () {
    var topic, topics, groupId, expectedCommittedOffset;
    topic = `_exist_topic_3_test`;
    topics = [ { topic: topic, partition: 0 } ];
    groupId = `_groupId_commit_v1_test`;
    before(function (done) {
      producer.send([{ topic, messages: ['firstMessage'] }], (err, data) => { console.log(`Producer sent data: ${JSON.stringify(data)}, err: ${JSON.stringify(err)}`); });
      createCGandCommitToLatestOffset(groupId, topic, (err, highWaterOffset) => {
        expectedCommittedOffset = highWaterOffset;
        done(err);
      });
    });

    it('should return -1 when the consumer group has no commits on the broker', function (done) {
      var groupIdNoCommits = groupId + '2';
      offset.fetchCommitsV1(groupIdNoCommits, topics, function (err, data) {
        data.should.be.ok;
        Object.keys(data)[0].should.equal(topic);
        data[topic][0].should.equal(-1);
        done(err);
      });
    });

    it('should get the last committed offset consumer group on the broker', function (done) {
      offset.fetchCommitsV1(groupId, topics, function (err, data) {
        data.should.be.ok;
        Object.keys(data)[0].should.equal(topic);
        data[topic][0].should.equal(expectedCommittedOffset);
        done(err);
      });
    });

    it('should keep calling fetchCommits until offset is ready', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [ { topic: topic, offset: 10 } ];
      offset.fetchCommitsV1('_groupId_commit_1_test', topics, done);
    });
  });

  describe('#fetchEarliestOffsets', function () {
    it('should callback with error if topic does not exist', function (done) {
      offset.fetchEarliestOffsets([uuid.v4()], function (error) {
        error.should.be.an.instanceOf(Error);
        error.message.should.be.exactly('Topic(s) does not exist');
        done();
      });
    });
  });

  describe('#fetchLatestOffsets', function () {
    it('should callback with error if topic does not exist', function (done) {
      offset.fetchLatestOffsets([uuid.v4()], function (error) {
        error.should.be.an.instanceOf(Error);
        error.message.should.be.exactly('Topic(s) does not exist');
        done();
      });
    });

    it('should get latest kafka offsets for all topics passed in', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [topic];
      var partition = 0;
      offset.fetch([{ topic: topic, time: -1 }], function (err, results) {
        if (err) return done(err);
        var latestOffset = results[topic][partition][0];
        offset.fetchLatestOffsets(topics, function (err, offsets) {
          if (err) return done(err);
          offsets[topic][partition].should.equal(latestOffset);
          done();
        });
      });
    });

    it('should keep calling fetchLatestOffsets until offset is ready', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [topic];
      offset.fetchLatestOffsets(topics, done);
    });
  });
});

const createCGandCommitToLatestOffset = (groupId, topic, cb) => {
  try {
    var consumerGroupOptions = {
      groupId: groupId,
      fromOffset: 'earliest',
      kafkaHost: 'localhost:9092',
      autoCommitIntervalMs: 1,
      autoCommit: true
    };
    var consumerGroup = new ConsumerGroup(consumerGroupOptions, topic);
    consumerGroup.on('message', (message) => {
      console.log('got message');
      console.log(`Got a message on the consumer group: ${JSON.stringify(message)}`);
      if (message.offset === message.highWaterOffset - 1) {
        setTimeout(() => {
          consumerGroup.close(true, () => {
            console.log('closed cg');
            return cb(null, message.highWaterOffset);
          });
        }, 0);
      }
    });
    consumerGroup.on('error', (err) => {
      return cb(err);
    });
  } catch (e) {
    return cb(e);
  }
};
