using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using RdKafka;

namespace ConsoleApplication
{
    public class Program
    {
        public static int N = 50;

        public static async Task RdKafka_Await()
        {
            using (RdKafka.Producer producer = new RdKafka.Producer("127.0.0.1:9092"))
            using (Topic topic = producer.Topic("testtopic"))
            {
                for (int i=0; i<N; ++i)
                {
                    byte[] data = Encoding.UTF8.GetBytes("Hello RdKafka");
                    DeliveryReport deliveryReport = await topic.Produce(data);
                    Console.WriteLine($"Produced to Partition: {deliveryReport.Partition}, Offset: {deliveryReport.Offset}");
                }
            }
            Console.WriteLine("End");
        }

        public static void RdKafka_WaitAll()
        {
            DateTime startTime = DateTime.Now;
            using (RdKafka.Producer producer = new RdKafka.Producer("127.0.0.1:9092"))
            using (Topic topic = producer.Topic("testtopic"))
            {
                Task<DeliveryReport>[] tasks = new Task<DeliveryReport>[N];
                for (int i=0; i<N; ++i)
                {
                    byte[] data = Encoding.UTF8.GetBytes("Hello RdKafka");
                    tasks[i] = topic.Produce(data);
                }
                Task.WaitAll(tasks);
            }
            Console.WriteLine("time to complete: " + (DateTime.Now - startTime).TotalMilliseconds.ToString());
        }

        public static async Task KafkaNet_Await()
        {
            var options = new KafkaOptions(new Uri("http://127.0.0.1:9092"));
            var router = new BrokerRouter(options);
            var client = new KafkaNet.Producer(router);

            for (int i=0; i<N; ++i)
            {
                await client.SendMessageAsync("testtopic", new[] { new KafkaNet.Protocol.Message("hello world")});
            }

            using (client) { }
        }

        public static void KafkaNet_WaitAll()
        {
            DateTime startTime = DateTime.Now;

            var options = new KafkaOptions(new Uri("http://127.0.0.1:9092"));
            var router = new BrokerRouter(options);
            var client = new KafkaNet.Producer(router);

            Task[] tasks = new Task[N];
            for (int i=0; i<N; ++i)
            {
                tasks[i] = client.SendMessageAsync("testtopic", new[] { new KafkaNet.Protocol.Message("hello world")});
            }

            using (client)
            {
                Task.WaitAll(tasks);
            }

            Console.WriteLine("time to complete: " + (DateTime.Now - startTime).TotalMilliseconds.ToString());
        }

        public static void Main(string[] args)
        {
            // KafkaNet_Await.Wait();
            // RdKafka_Await.Wait();

            //KafkaNet_WaitAll();
            RdKafka_WaitAll();

            // Results on my mac book pro (ms):
            // N          RdKafka    KafkaNet
            // 50          ~1200         ~250
            // 500000      ~1800        ~7200
            // 5000000    ~10000       ~68000
        }
    }
}
