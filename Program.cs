using System;
using System.Net;
using System.Threading;
using Confluent.Kafka;
class Program 
{ 
    static void Main(string[] args) 
    {
        var conf = new ConsumerConfig
        { 
            GroupId = "test-gators-group",
            BootstrapServers = "localhost:9092",
            ClientId = Dns.GetHostName(),
        };

        using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
        {
            c.Subscribe("gators");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    try
                    {
                        var cr = c.Consume(cts.Token);
                        Console.WriteLine($"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}' with id: '{cr.Message.Key}'.");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                c.Close();
            }
        } 
    } 
} 