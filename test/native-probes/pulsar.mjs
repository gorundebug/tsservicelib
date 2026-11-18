// Official-client suitability probe, not an SA runtime acceptance claim.
// Install pulsar-client@1.18.0 in an isolated directory and set PULSAR_NODE_MODULE.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { randomUUID, pbkdf2 } from 'node:crypto';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
const require = createRequire(import.meta.url);
const native = require(process.env.PULSAR_NODE_MODULE ?? 'pulsar-client');
const admin = process.env.PULSAR_TEST_ADMIN_URL;
assert.ok(admin, 'PULSAR_TEST_ADMIN_URL required');
const namespace = `public/sa-node-probe-${randomUUID()}`;
const topic = `persistent://${namespace}/events`;
const watchdog = setTimeout(() => { console.error('probe exceeded 40s; native lifecycle did not complete'); process.exit(1); }, 40_000);
async function request(method, path, value) {
 const response = await fetch(admin + path, {method, headers:{'content-type':'application/json'}, ...(value === undefined ? {} : {body:JSON.stringify(value)}), signal:AbortSignal.timeout(5000)});
 if(method === 'DELETE' && response.status === 404) return;
 assert.ok(response.ok, `${method} ${path}: ${response.status} ${await response.text()}`);
}
let client, producer, consumer;
try {
 await request('PUT',`/admin/v2/namespaces/${namespace}`,{});
 await request('POST',`/admin/v2/namespaces/${namespace}/autoTopicCreation`,{allowAutoTopicCreation:false});
 await request('POST',`/admin/v2/namespaces/${namespace}/autoSubscriptionCreation`,{allowAutoSubscriptionCreation:false});
 await request('PUT',`/admin/v2/persistent/${namespace}/events`);
 await request('PUT',`/admin/v2/persistent/${namespace}/events/subscription/workers`,{ledgerId:-1,entryId:-1,partitionIndex:-1});
 client = new native.Client({serviceUrl:process.env.PULSAR_TEST_URL ?? 'pulsar://127.0.0.1:16650',operationTimeoutSeconds:2,connectionTimeoutMs:1000,log:()=>{}});
 const options={topic,subscription:'workers',subscriptionType:'Shared',receiverQueueSize:1,receiverQueueSizeAcrossPartitions:1,nAckRedeliverTimeoutMs:100,batchIndexAckEnabled:true};
 consumer=await client.subscribe(options);
 producer=await client.createProducer({topic,batchingEnabled:false,blockIfQueueFull:false,maxPendingMessages:1,maxPendingMessagesAcrossPartitions:1,sendTimeoutMs:1000});
 const payload=Buffer.from([0,255,254,1]);
 const id=await producer.send({data:payload,partitionKey:'business-key',properties:{trace:'native-probe'}});
 let message=await consumer.receive();
 assert.deepEqual(message.getData(),payload);
 assert.deepEqual(message.getMessageId().serialize(),id.serialize());
 assert.equal(message.getPartitionKey(),'business-key');
 assert.equal(message.getProperties().trace,'native-probe');
 // No ACK: close must preserve the durable subscription and cause redelivery.
 await consumer.close();consumer=await client.subscribe(options);
 message=await consumer.receive();assert.deepEqual(message.getMessageId().serialize(),id.serialize());
 consumer.negativeAcknowledge(message);
 message=await consumer.receive();assert.deepEqual(message.getMessageId().serialize(),id.serialize());
 await consumer.acknowledge(message);
 // Four pending native receives must not occupy the default libuv worker pool.
 // receive(timeout) uses AsyncWorker; receive() uses the official async C API.
 const pending=Array.from({length:4},()=>consumer.receive().then(()=>({unexpected:true}),error=>({error:String(error)})));
 let ticks=0;const ticker=setInterval(()=>{ticks++;},5);
 const before=performance.now();
 try {await promisify(pbkdf2)('probe','salt',1000,32,'sha256');await delay(50);} finally {clearInterval(ticker);}
 assert.ok(ticks>=2,'event loop stalled during native receive');
 assert.ok(performance.now()-before<1000,'native receive monopolized libuv workers');
 const closing=performance.now();await consumer.close();consumer=undefined;
 for(const result of await Promise.all(pending)) assert.ok(result.error,'closed receive unexpectedly delivered data');
 const closeMs=performance.now()-closing;assert.ok(closeMs<2000,`close too slow: ${closeMs}`);
 await producer.close();producer=undefined;await client.close();client=undefined;
 console.log(JSON.stringify({native:'pulsar-client@1.18.0',node:process.version,arch:process.arch,binary:true,explicitAck:true,redelivery:true,nack:true,pendingReceives:4,eventLoopTicks:ticks,closeMs}));
} finally {
 if(consumer) await consumer.close();if(producer) await producer.close();if(client) await client.close();
 await request('DELETE',`/admin/v2/persistent/${namespace}/events?force=true`);
 await request('DELETE',`/admin/v2/namespaces/${namespace}`);
 clearTimeout(watchdog);
}
