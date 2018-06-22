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
      producer.createTopics(['_exist_topic_3_test', '_exist_topic_no_messages'], true, function (err) {
        console.log(`GOT AN ERROR CREATING TOPIC: ${JSON.stringify(err)}`);
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
      var topics = [{ topic: topic }];
      offset.fetch(topics, function (err, data) {
        var offsets = data[topic][0];
        console.log(`OFFSET FETCH LOG data: ${JSON.stringify(data)}`);
        console.log(`OFFSET FETCH LOG err: ${err}`);
        offsets.should.be.an.instanceOf(Array);
        offsets.length.should.equal(1);
        done(err);
      });
    });

    it('should return earliest offset of the topics', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [{ topic: topic, time: -2 }];
      offset.fetch(topics, function (err, data) {
        var offsets = data[topic][0];
        offsets.should.be.an.instanceOf(Array);
        offsets.length.should.above(0);
        done(err);
      });
    });

    it('should return latest offset of the topics', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [{ topic: topic, time: -1 }];
      offset.fetch(topics, function (err, data) {
        var offsets = data[topic][0];
        offsets.should.be.an.instanceOf(Array);
        offsets.length.should.above(0);
        done(err);
      });
    });

    it('should keeping calling fetch until offset is ready', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [{ topic: topic }];
      offset.fetch(topics, done);
    });
  });

  describe('#commit', function () {
    it('should commit successfully', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [{ topic: topic, offset: 10 }];
      offset.commit('_groupId_commit_test', topics, function (err, data) {
        data.should.be.ok;
        Object.keys(data)[0].should.equal(topic);
        done(err);
      });
    });

    it('should keep calling commit until offset is ready', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [{ topic: topic, offset: 10 }];
      offset.commit('_groupId_commit_test', topics, done);
    });
  });

  describe('#fetchCommits', function () {
    it('should get last committed offset of the consumer group', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [{ topic: topic, offset: 10 }];
      offset.fetchCommits('_groupId_commit_1_test', topics, function (err, data) {
        data.should.be.ok;
        Object.keys(data)[0].should.equal(topic);
        data[topic][0].should.equal(-1);
        done(err);
      });
    });

    it('should keep calling fetchCommits until offset is ready', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [{ topic: topic, offset: 10 }];
      offset.fetchCommits('_groupId_commit_1_test', topics, done);
    });
  });

  describe.only('#fetchCommitsV1', function () {
    var topic, topics, groupId, consumerGroup, expectedCommittedOffset;
    topic = `_exist_topic_3_test`;
    topics = [{ topic: topic, partition: 0 }];
    groupId = `_groupId_commit_v1_1_test1`;
    before(function (done) {
      var consumerGroupOptions = {
        groupId: groupId,
        fromOffset: 'earliest',
        kafkaHost: 'localhost:9092',
        autoCommitIntervalMs: 1,
        autoCommit: true
      };
      // autoCommitIntervalMs
      // commitOffsetsOnFirstJoin: false
      consumerGroup = new ConsumerGroup(consumerGroupOptions, '');
      consumerGroup.pause();
      consumerGroup.on('message', (message) => {
        console.log('got message');
        console.log(`Got a message on the consumer group: ${JSON.stringify(message)}`);
        if (message.offset === message.highWaterOffset - 1) {
          setTimeout(() => {
            consumerGroup.commit((err, data) => {
              console.log(`consumer committed manually : ${data}, err: ${err}`);
              expectedCommittedOffset = message.highWaterOffset;
              consumerGroup.close(true, () => {
                console.log('closed cg');
                done();
              });
            });
          }, 0);
        }
      });
      consumerGroup.on('error', (err) => {
        console.log(`ERROR ON CONSUMER GROUP: ${JSON.stringify(err)}`);
      });
      consumerGroup.on('offsetOutOfRange', (payload) => {
        console.log(`There was an offsetOutOfRange for ${JSON.stringify(payload)}`);
      });

      // producer.createTopics([topic], true, function (err, result) {
      // console.log(`Tried to create topic: ${result}, err: ${err}`);
      consumerGroup.once('connect', () => {
        // producer.send([{ topic, messages: ['firstMessage'] }], (err, data) => { consumerGroup.resume(); console.log(`Producer sent data: ${JSON.stringify(data)}, err: ${JSON.stringify(err)}`); });
        producer.send([{ topic, messages: ['firstMessage'] }], (err, data) => { consumerGroup.resume(); consumerGroup.addTopics([topic], (error, result) => { console.log(`added our topic: ${result}, err: ${error}`); }); console.log(`Producer sent data: ${JSON.stringify(data)}, err: ${JSON.stringify(err)}`); });
      });
      // });
    });

    xit('should return -1 when the consumer group has no commits on the broker', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [{ topic: topic }];
      var groupId = '_groupId_commit_v1_1_test';
      offset.fetchCommitsV1(groupId, topics, function (err, data) {
        console.log(`fetchCommtsV1 data!!! ${JSON.stringify(data)}`);
        console.log(`fetchCommtsV1 err ${JSON.stringify(err)}`);
        data.should.be.ok;
        Object.keys(data)[0].should.equal(topic);
        data[topic][0].should.equal(-1);
        done(err);
      });
    });

    // it('should be able to do it without me', (done) => {
    //   console.log('trying to fetch with cg');
    //   consumerGroup.fetchOffset({ '_exist_topic_3_test': [{ partition: 0 }] }, (err, res) => {
    //     console.log(`FROM THE CG THEMSELF: ${JSON.stringify(res)}, err: ${err}`);
    //     done(err);
    //   });
    // });

    it('give me the topic offset stuff', (done) => {
      var nowtopics = [topic];
      offset.fetchLatestOffsets(nowtopics, function (err, offsets) {
        if (err) return done(err);
        console.log(`OFFSETS FROM FETCH: ${JSON.stringify(offsets)}`);
        // offsets[topic][partition].should.equal(latestOffset);
        done(err);
      });
      // });
    });

    it('should get the last committed offset consumer group on the broker', function (done) {
      console.log('offset tfetch');
      offset.fetchCommitsV1(groupId, topics, function (err, data) {
        console.log(`AFTER COMMIT MESSAGE fetchCommtsV1 data!!! ${JSON.stringify(data)}`);
        console.log(`AFTER COMMIT MESSAGE fetchCommtsV1 err ${JSON.stringify(err)}`);
        offset.fetchCommits(groupId, topics, (error, dat) => {
          console.log(`V0 FetchCommits: ${JSON.stringify(dat)}, err: ${error}`);
          data.should.be.ok;
          Object.keys(data)[0].should.equal(topic);
          data[topic][0].should.equal(expectedCommittedOffset);
          done(err);
        });
      });
    });

    xit('should keep calling fetchCommits until offset is ready', function (done) {
      var topic = '_exist_topic_3_test';
      var topics = [{ topic: topic, offset: 10 }];
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
