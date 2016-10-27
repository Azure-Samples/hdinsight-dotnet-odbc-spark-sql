using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.Odbc;
using System.Diagnostics;

namespace ODBCClient
{
    class Program
    {

        static void Main(string[] args)
        {
            // Enter Username, Password and cluster name of your cluster
            var userName = "FILL_YOUR_USERNAME_HERE";
            var password = "FILL_YOUR_PASSWORD_HERE";
            var dnsName = "FILL_YOUR_CLUSTERNAME_HERE";
        
            const string defaultAgentPrefix = "HDInsightODBClient";
            const int totalIteration = 1;

            try
            {
                for (int j = 0; j < totalIteration; j++)
                {
                    string defaultAgentString = defaultAgentPrefix + j;

                    var connectionString = GenerateConnectionString(defaultAgentString, userName, password, dnsName);
                    using (var connection = new OdbcConnection(connectionString))
                    {
                        connection.Open();
                        Stopwatch stopwatch = new Stopwatch();

                        List<string> queryList = new List<string>();
                        Console.WriteLine("StartTime:" + DateTime.Now);

                        for (int i = 0; i < 1; i++)
                        {
                            queryList.Add("show tables");
                            queryList.Add("DROP TABLE mysampletable");
                            queryList.Add("CREATE EXTERNAL TABLE mysampletable (deviceplatform string, count string) ROW " +
                                "FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE");
                            queryList.Add("INSERT OVERWRITE TABLE mysampletable SELECT deviceplatform, COUNT(*) as count " +
                                "FROM hivesampletable GROUP BY deviceplatform");
                            queryList.Add("select * from mysampletable");
                        }

                        List<long> queryTime = new List<long>();

                        using (OdbcCommand command = connection.CreateCommand())
                        {
                            command.CommandTimeout = 300;

                            foreach (var query in queryList)
                            {
                                command.CommandText = query;
                                Console.WriteLine(query);
                                stopwatch.Start();
                                OdbcDataReader reader = command.ExecuteReader();
                                PrintOutput(reader);
                                stopwatch.Stop();
                                Console.WriteLine("Query took : " + stopwatch.Elapsed + "seconds");
                                queryTime.Add(stopwatch.ElapsedMilliseconds);
                                stopwatch.Reset();
                                reader.Close();
                            }
                        }

                        Console.WriteLine("AverageTime in ms:" + queryTime.Average());
                        Console.WriteLine("MaxTime in ms:" + queryTime.Max());
                        Console.WriteLine("MinTime in ms:" + queryTime.Min());
                        Console.WriteLine("Press ENTER to exit..");
                        Console.ReadLine();
                        connection.Close();
                    }

                }

            } catch (OdbcException)
            {
                Console.WriteLine("Invalid Clustername/credentials. Please check your admin credentials" +
                   " and clustername");
                Console.WriteLine("Press ENTER to exit..");
                Console.ReadLine();
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
            }



        }

        //Generates Microsoft Spark ODBC Connection String to connect to your cluster
        private static string GenerateConnectionString(string defaultUserAgent, string userName, string password, string dnsName)
        {

            var hiveOdbcDriverName = "Microsoft Spark ODBC Driver";
            var domainName = "azurehdinsight.net";
            var servicePortNumber = "443";

            //By default this is set to 10 minutes. Change it to a higher value if your queries take longer to run
            var timeoutInSeconds = 600;

            return
                string.Format(
                @"DRIVER={0};HOST={1}.{2};PORT={3};SparkServerType=3;AuthMech=6;UID={4};PWD={5};" +
                @"UseNativeQuery=1;WdHttpUserAgent={6};WdSocketTimeout={7};", 
                    "{" + hiveOdbcDriverName + "}",
                    dnsName,
                    domainName,
                    servicePortNumber,
                    // Using curly braces to escape special chars in user name and password
                    "{" + userName + "}",
                    "{" + password + "}",
                    defaultUserAgent,
                    timeoutInSeconds
                    );

        }

        //Prints output from queries
        public static void PrintOutput(OdbcDataReader reader)
        {
            while (reader.Read())
            {
                for (int ordinal = 0; ordinal < reader.FieldCount; ordinal++)
                    Console.Write(reader[ordinal]+"\t");
                Console.WriteLine();
            }
            
        }

    }
}
