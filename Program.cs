using System;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using RdKafka;

namespace ConsoleApplication
{
    public class Program
    {
        public static async Task _main()
        {
            Console.WriteLine("Start");
            using (RdKafka.Producer producer = new RdKafka.Producer("127.0.0.1:9092"))
            using (Topic topic = producer.Topic("testtopic"))
            {
                for (int i=0; i<100000; ++i)
                {
                    byte[] data = Encoding.UTF8.GetBytes("Hello RdKafka");
                    DeliveryReport deliveryReport = await topic.Produce(data);
                    Console.WriteLine($"Produced to Partition: {deliveryReport.Partition}, Offset: {deliveryReport.Offset}");
                }
            }
            Console.WriteLine("End");
        }

        public static async Task _main2()
        {
            var options = new KafkaOptions(new Uri("http://127.0.0.1:9092"));
            var router = new BrokerRouter(options);
            var client = new KafkaNet.Producer(router);

            for (int i=0; i<100000; ++i)
            {
                await client.SendMessageAsync("testtopic", new[] { new KafkaNet.Protocol.Message("hello world")});
            }

            using (client) { }
        }

        public static void Main(string[] args)
        {
            _main().Wait();
        }
    }
}
