using SqlServiceBrokerListener;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TestApp
{
    public class Program
    {
        public static string conn = "Data Source=WORKLAPTOP\\SQLEXPRESS;Integrated Security=True";
        public static void Main(string[] args)
        {
            try
            {
                CreateDatabase();
                Thread.Sleep(1000);

                Init();
                Process();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            { 
                DeInit();
            }
          
        }

        public static void Process()
        {
            bool quit = false;
            while (!quit)
            {
                Console.WriteLine("Input Message or quit:");
                string message = Console.ReadLine();
                if (message.ToLower() == "quit")
                    quit = true;
                SendMessage(message);
            }
        }

        public static void CreateDatabase()
        {
            var sqlCommand = "use master; CREATE DATABASE ServiceBrokerTest; ";
            ExecuteCommand(sqlCommand.Replace('\n', ' ').Replace('\r', ' '));
        }

        public static void Init()
        {
            var sqlCommand = @"
USE ServiceBrokerTest;
ALTER DATABASE ServiceBrokerTest SET ENABLE_BROKER;
CREATE MESSAGE TYPE SBMessage
VALIDATION = NONE
CREATE CONTRACT SBContract
(SBMessage SENT BY INITIATOR)
CREATE QUEUE SBSendQueue
CREATE QUEUE SBReceiveQueue
CREATE SERVICE SBSendService
ON QUEUE SBSendQueue (SBContract)
CREATE SERVICE SBReceiveService
ON QUEUE SBReceiveQueue (SBContract)
                    ";
            ExecuteCommand(sqlCommand.Replace('\n',' ').Replace('\r',' '));
         }


        static void DeInit()
        {
            var sqlCommand = @"
                    USE master
                    DROP DATABASE ServiceBrokerTest
  
                    ";
            ExecuteCommand(sqlCommand);
        }

        public static void SendMessage(string message)
        {
            string sendCommand = $@"
USE ServiceBrokerTest;
DECLARE @SBDialog uniqueidentifier
DECLARE @Message NVARCHAR(128)
BEGIN DIALOG CONVERSATION @SBDialog
FROM SERVICE SBSendService
TO SERVICE 'SBReceiveService'
ON CONTRACT SBContract
WITH ENCRYPTION = OFF
SET @Message = N'{message}';
SEND ON CONVERSATION @SBDialog
MESSAGE TYPE SBMessage (@Message)
            ";
            ExecuteCommand(sendCommand);
        }
        public static void ExecuteCommand(string command)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(conn))
                using (SqlCommand sqlCommand = new SqlCommand(command, connection))
                {
                    connection.Open();
                    sqlCommand.CommandType = System.Data.CommandType.Text;
                    sqlCommand.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {

                Console.WriteLine(e.Message);
            }   
            
        }
    }
}
