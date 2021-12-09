using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Data.SqlClient;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Azure.KeyVault;
using System.Net.Http;

namespace BucketDeleteApp
{
    public static class BucketDelete
    {
        [FunctionName("BucketDelete")]
        public static void Run([TimerTrigger("* * * * * *")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"BucketDelete function started at: {DateTime.Now}");
            

            log.LogInformation($"BucketDelete function connecting keyvault at : {DateTime.Now}");
            string connectionStringKeyVault = string.Format("RunAs=App;AppId={0};{1}{2};{3}{4}", Environment.GetEnvironmentVariable("ClientID"), "TenantId=", Environment.GetEnvironmentVariable("TenantID"), "AppKey=", Environment.GetEnvironmentVariable("ClientSecret"));
            var azureServiceTokenProvider = new AzureServiceTokenProvider(connectionStringKeyVault);
            string s3SecretKey, s3Id, s3Url, connectionString;
            var keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback.Invoke));
            s3Id = keyVaultClient.GetSecretAsync(string.Format("{0}{1}", Environment.GetEnvironmentVariable("keyVaultURI"), "S3ID")).Result.Value;
            s3SecretKey = keyVaultClient.GetSecretAsync(string.Format("{0}{1}", Environment.GetEnvironmentVariable("keyVaultURI"), "S3Secret")).Result.Value;
            s3Url = keyVaultClient.GetSecretAsync(string.Format("{0}{1}", Environment.GetEnvironmentVariable("keyVaultURI"), "S3URL")).Result.Value;
            log.LogInformation($"Reading from keyvault completed at : {DateTime.Now}");

            var str = keyVaultClient.GetSecretAsync(string.Format("{0}{1}", Environment.GetEnvironmentVariable("keyVaultURI"), "sSQLConnectionString")).Result.Value;  //Environment.GetEnvironmentVariable("sqldb_connection");

            log.LogInformation($"BucketDelete function connecting database at : {DateTime.Now}");
            using (SqlConnection conn = new SqlConnection(str))
            {
                log.LogInformation($"BucketDelete function database successfully connected at : {DateTime.Now}");
                //conn.Open();
                var text = @"select dr.UnitId, st.DigitalFile, st.username from DeleteRequest dr 
                            inner join StorageUnit st on dr.UnitId = st.UnitId 
                            where dr.Process = 0";

                SqlCommand SelectCommand = new SqlCommand(text, conn);
                DataTable dtResult = new DataTable();
                SqlDataAdapter adapter = new SqlDataAdapter();
                adapter.SelectCommand = SelectCommand;
                adapter.Fill(dtResult);

                log.LogInformation($"BucketDelete function reading from database, " + dtResult.Rows.Count.ToString() + " records found");

                if (dtResult != null && dtResult.Rows.Count > 0)
                {
                    foreach (DataRow row in dtResult.Rows)
                    {
                        var unitId = row["UnitId"].ToString();
                        var digitalFile = row["DigitalFile"].ToString();
                        var username = row["username"].ToString();

                        log.LogInformation($"Deleting file : " + digitalFile + " - unitid : " + unitId );

                        var response = DeleteFile(digitalFile);

                        log.LogInformation($"file : " + digitalFile + " - unitid : " + unitId + " deleted successfully, response was : " + response);

                        string updateQuery = "Update DeleteRequest Set Process = 1 where UnitId = @unitId;";
                        string deleteQuery = "Delete from StorageUnit where UnitId = @unitId;";
                        SqlCommand updateCommand = new SqlCommand(updateQuery + deleteQuery, conn);
                        updateCommand.Parameters.AddWithValue("@unitId", unitId);

                        if (conn.State == ConnectionState.Open)
                            conn.Close();

                        conn.Open();
                        updateCommand.ExecuteNonQuery();
                        conn.Close();

                        CallPostApi(digitalFile, username);

                        log.LogInformation($"BucketDelete function database successfully updated after deleting the file.");
                    }
                }

            }
        }

        private static HttpResponseMessage CallPostApi(string digitalFile, string username)
        {
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(Environment.GetEnvironmentVariable("ApiUrl"));

                //creating object
                PostDeleteRequest obj = new PostDeleteRequest()
                {
                    ClientCode = "AAA",
                    Program = "DELETE",
                    Description1 = "N",
                    Description2 = digitalFile,
                    Username = username
                };

                //HTTP POST
                var postTask = client.PostAsJsonAsync("web/common/DigitalFile", obj);
                postTask.Wait();

                return postTask.Result;                
            }
        }

        private static DeleteObjectResponse DeleteFile(string filename, string s3SecretKey, string s3Id, string s3Url)
        {
            

            var config = new AmazonS3Config();
            config.ServiceURL = s3Url; 
            config.ForcePathStyle = true;
            var s3Client = new AmazonS3Client(s3Id, s3SecretKey, config);

            String key = filename;
            string _foldername = Environment.GetEnvironmentVariable("aws_foldername"); //"images";
            string _keyPublic = Environment.GetEnvironmentVariable("s3Id");
            string _keySecret = Environment.GetEnvironmentVariable("s3SecretKey");
            string _bucket = Environment.GetEnvironmentVariable("aws_bucket");

            var deleteObjectRequest = new DeleteObjectRequest { BucketName = _bucket, Key = String.Format("{0}/{1}", _foldername, key) };
            var response = s3Client.DeleteObjectAsync(deleteObjectRequest).GetAwaiter().GetResult();

            return response;
        }

    }
}
