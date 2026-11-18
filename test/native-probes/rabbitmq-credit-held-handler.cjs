const rabbit = require('rabbitmq-amqp-js-client');
const { randomUUID } = require('node:crypto');
const sleep = ms => new Promise(r => setTimeout(r,ms));
const watchdog = setTimeout(()=>{console.error('probe timeout');process.exit(2)},20000);
(async()=>{
 const env=rabbit.createEnvironment({host:'127.0.0.1',port:Number(process.env.RABBITMQ_PROBE_PORT || 15672),username:'adapter',password:'adapter-test'});
 const connection=await env.createConnection({reconnect:false});
 const management=connection.management();
 const queue='sa-credit-probe-'+randomUUID();
 let consumer,publisher,release;
 const gate=new Promise(r=>release=r);
 let received=0,completed=0,accepted=0;
 try {
  await management.declareQueue(queue);
  publisher=await connection.createPublisher({queue:{name:queue}});
  for(let start=0;start<2000;start+=50) await Promise.all(Array.from({length:50},(_,i)=>publisher.publish(rabbit.createAmqpMessage({body:String(start+i)}))));
  consumer=await connection.createConsumer({queue:{name:queue},messageHandler:async context=>{
   received++;
   await gate;
   context.accept();accepted++;completed++;
  }});
  consumer.start();
  const deadline=Date.now()+4000;
  while(received<2000 && Date.now()<deadline) await sleep(10);
  console.log(JSON.stringify({phase:'gate_closed',published:2000,received,completed,accepted}));
  release();await sleep(250);
  console.log(JSON.stringify({phase:'gate_open',received,completed,accepted}));
 } finally {
  release();consumer?.close();publisher?.close();
  await management.deleteQueue(queue);await connection.close();await env.close();clearTimeout(watchdog);
 }
})().catch(e=>{console.error(e);process.exit(1)});
