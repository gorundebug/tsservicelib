import assert from 'node:assert/strict';
import {test} from 'node:test';
import {makeTestEnvironment} from '../../dist-test/test/support/environment.js';
import {Context,errorSerdeType} from '../../dist/runtime/index.js';
import {makeInputStream} from '../../dist/operators/index.js';
import {TestMetrics} from '../../dist/runtime/testmetrics/index.js';
const cases=[['nats',7,'NATS'],['jetstream',8,'NATS'],['pulsar',11,'Pulsar'],['redisstreams',10,'RedisStreams'],['rabbitmq',9,'RabbitMQ']];
for(const [protocol,type,prefix] of cases) await test(`${protocol} source observations precede BeginRequest`,async()=>{
 const module=await import(`../../dist/datasource/${protocol==='jetstream'?'nats':protocol}/index.js`);
 const metrics=new TestMetrics();
 const implementation=({nats:'@nats-io/transport-node',jetstream:'@nats-io/transport-node',pulsar:'pulsar-client-node',redisstreams:'node-redis',rabbitmq:'rabbitmq-amqp-js-client'})[protocol];
 const options=({nats:{subject:'events',createStream:false,createConsumer:false},jetstream:{subject:'events',streamName:'events',durableName:'workers',createStream:false,createConsumer:false},pulsar:{topic:'persistent://public/default/events',subscriptionName:'workers'},redisstreams:{streamKey:'events',consumerGroup:'workers',consumerName:'one'},rabbitmq:{queueName:'events',createQueue:false,createExchange:false,createBinding:false}})[protocol];
 const server=({nats:'nats://localhost',jetstream:'nats://localhost',pulsar:'pulsar://localhost',redisstreams:'redis://localhost',rabbitmq:'amqp://localhost'})[protocol];
 const config={id:1,name:'input',type:'Input',pipeline:'main',properties:{},idService:1,idSource:0,idSources:[],xPos:0,yPos:0,idEndpoint:100,valueType:'string'};
 const env=makeTestEnvironment([config],{metrics,dataConnectors:[{id:10,name:'events',type,properties:{},servers:[server],implementation,redisDatabase:0,adminUrl:'http://localhost',connectTimeoutMs:1000,operationTimeoutMs:1000}],
 endpoints:[{id:100,name:'read',idDataConnector:10,enabled:true,properties:{},...options}]});
 env.serdeRegistry().registerStreamErrorType(1,errorSerdeType);
 const input=makeInputStream(config,env);
 const count=(observation)=>metrics.counterValue('broker_endpoint_source_observations_total',{protocol,connector:'events',endpoint:'read',observation});
 const seen=[];
 module[`make${prefix}EndpointConsumer`](input,{
  beginRequest(){seen.push(count('received'));throw new Error('business begin failed');},
  onError(){},getMessageId(){return '';},endRequest(){assert.fail('failed begin must not end');},consumeMessage(){assert.fail('failed begin must not consume');}
 });
 const endpoint=env.dataSourceById(10).endpoints()[0];
 for(let i=0;i<2;i++) {
  const message={message:{headers:new Map(),getProperties:()=>({}),getRedeliveryCount:()=>i,application_properties:{},delivery_count:i,fields:[]},retire(){}};
  if(protocol==='jetstream')message.jetstream={info:{deliveryCount:i+1}};
  await endpoint.handle(new Context(),message);
 }
 assert.deepEqual(seen,[1,2]);
 assert.equal(count('received'),2);
 if(['jetstream','pulsar','rabbitmq'].includes(protocol))assert.equal(count('redelivered'),1);
 if(protocol==='redisstreams') { endpoint.observations.observe(new Context(),'reclaimed');assert.equal(count('reclaimed'),1); }
});

await test('source observations use the production Prometheus exporter', async () => {
 const {SourceMetrics} = await import('../../dist/internal/broker-source-metrics.js');
 const {PrometheusMetrics,PrometheusMetricsEngine} = await import('../../dist/runtime/index.js');
 const metrics = new PrometheusMetrics();
 const profiles = {nats:['received'],jetstream:['received','redelivered'],pulsar:['received','redelivered'],redisstreams:['received','reclaimed'],rabbitmq:['received','redelivered']};
 for (const [protocol, observations] of Object.entries(profiles)) {
  const source = new SourceMetrics(metrics,protocol,'events','read');
  for (const observation of observations) source.observe(new Context(),observation);
 }
 const text = await new PrometheusMetricsEngine(metrics).render();
 const lines = text.split('\n').filter(line=>line.startsWith('broker_endpoint_source_observations_total{') && line.endsWith(' 1'));
 assert.equal(lines.length,9);
 for (const [protocol, observations] of Object.entries(profiles)) for (const observation of observations) {
  assert.equal(lines.filter(line=>line.includes(`protocol="${protocol}"`) && line.includes(`observation="${observation}"`) && line.includes('connector="events"') && line.includes('endpoint="read"')).length,1);
 }
 assert.ok(!text.includes('source_observations_total_total'));
});
