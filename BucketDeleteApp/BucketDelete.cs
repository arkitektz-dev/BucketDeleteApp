using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Data.Common;
using Amazon.S3;
using Amazon.S3.Model;

namespace BucketDeleteApp
{
    public static class BucketDelete
    {
        [FunctionName("BucketDelete")]
        public static void Run([TimerTrigger("* * * * * *")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"BucketDelete function started at: {DateTime.Now}");
            var str = Environment.GetEnvironmentVariable("sqldb_connection");

            log.LogInformation($"BucketDelete function connecting database at : {DateTime.Now}");
            using (SqlConnection conn = new SqlConnection(str))
            {
                log.LogInformation($"BucketDelete function database successfully connected at : {DateTime.Now}");
                //conn.Open();
                var text = @"select dr.UnitId, st.DigitalFile from DeleteRequest dr 
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

                        log.LogInformation($"Deleting file : " + digitalFile + " - unitid : " + unitId );

                        var response = DeleteFile(digitalFile);

                        log.LogInformation($"file : " + digitalFile + " - unitid : " + unitId + " deleted successfully, response was : " + response.Status.ToString());

                        string updateQuery = "Update DeleteRequest Set Process = 1 where UnitId = @unitId;";
                        string deleteQuery = "Delete from StorageUnit where UnitId = @unitId;";
                        SqlCommand updateCommand = new SqlCommand(updateQuery + deleteQuery, conn);
                        updateCommand.Parameters.AddWithValue("@unitId", unitId);

                        if (conn.State == ConnectionState.Open)
                            conn.Close();

                        conn.Open();
                        updateCommand.ExecuteNonQuery();
                        conn.Close();

                        log.LogInformation($"BucketDelete function database successfully updated after deleting the file.");
                    }
                }

            }
        }


        public static async Task<DeleteObjectResponse> DeleteFile(String filename)
        {
            String key = filename;
            string _foldername = "images";
            string _keyPublic = Environment.GetEnvironmentVariable("aws_accesskeyid");
            string _keySecret = Environment.GetEnvironmentVariable("aws_secretkey");
            string _bucket = Environment.GetEnvironmentVariable("aws_bucket");
            var amazonClient = new AmazonS3Client(_keyPublic, _keySecret, Amazon.RegionEndpoint.APSouth1);

            var deleteObjectRequest = new DeleteObjectRequest { BucketName = _bucket, Key = String.Format("{0}/{1}", _foldername, key) };
            var response = amazonClient.DeleteObjectAsync(deleteObjectRequest).GetAwaiter().GetResult();

            return response;
        }
    }
}
