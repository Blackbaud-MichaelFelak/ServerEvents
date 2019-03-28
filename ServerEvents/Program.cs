using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using CsvHelper;

namespace ServerEvents
{
    public class Program
    {
        #region Fields

        #region MaverickServices
        private static IEnumerable<string> MaverickServices = new List<string>()
        {
            //"Application Lookup Data Service",
            //"Constituent Analysis Database",
            //"Constituent Analysis Processing Service",
            //"Constituent List Data Service",
            //"Constituent Lists UI Service",
            //"Data Mart Data Service",
            //"Data Mart Database",
            //"Data Mart UI Service",
            //"Data Mart Upload Processing Service",
            //"Fundraising Database",
            //"Fundraising ETL Processing Service",
            //"Fundraising Health UI Service",
            //"Fundraising Message Consumer Service",
            //"Fundraising Rules Data Service",
            //"Fundraising Rules Database",
            //"Fundraising Rules Output Data Service",
            //"Fundraising Rules Output Database",
            //"Fundraising Rules UI Service",
            //"Fundraising SSIS Database",
            //"Fundraising UI Module",
            //"Fundraising View Data Service",
            //"Gift List Data Service",
            //"Giving Database",
            //"Giving View Data Service",
            //"Giving View UI Service",
            //"Host Service",
            //"List API Service",
            //"List Management Data Service",
            //"List Management Database",
            //"NXT Onboard Data Service",
            //"NXT Onboard Database",
            //"Nxt Onboard Host Service",
            //"NXT Onboard UI Module",
            //"Pod Info Data Service",
            //"Pod Lookup Data Service",
            //"RE7 Data Extraction Database",
            "RE7 Data Extraction Service",
            //"RE7 Data Migrations Processing Service",
            //"RE7 Data Migrations Database",
            //"Shell Service"
        };
        #endregion MaverickServices

        #region DetailsStrings
        private static IEnumerable<string> GetTenantIdStrings = new List<string>()
        {
            "tenantid=",
            "tenant with ID '",
            "find tenant ",
            "Constituent tag processing failed for tenant "
        };
        #endregion DetailsStrings

        static string logFileName = $"{AppDomain.CurrentDomain.BaseDirectory}/log.txt";

        #endregion Fields

        internal static bool log = true;

        static void Main(string[] args)
        {
            var cb = GetConnectionString();
            var sw = new Stopwatch();

            using (var connection = new SqlConnection(cb.ConnectionString))
            {
                connection.Open();
                LogToFile("*****NEW SERVER EVENTS IMPORT*****");
                LogToFile($"Log start time: {System.DateTime.Now}\n");
                sw.Start();
                ImportCsv(connection);
                sw.Stop();
                Console.WriteLine($"CSV imported to staging in {sw.ElapsedMilliseconds} ms.");
                sw.Reset();

                sw.Start();
                SubmitServerEventsSQL(connection);
                sw.Stop();
                Console.WriteLine($"Uploading server events to Azure completed in {sw.ElapsedMilliseconds} ms.");
                sw.Reset();

                sw.Start();
                ClearStagingTable(connection);
                sw.Stop();
                Console.WriteLine($"Staging table cleared in {sw.ElapsedMilliseconds} ms.");
                sw.Reset();

                connection.Close();
            }

            Console.WriteLine("Press enter to close...");
            Console.ReadLine();
        }

        #region Private

        private static void ImportCsv(SqlConnection connection)
        {
            Console.Write("Enter path for .csv to import (or leave blank for no import): ");
            var path = Console.ReadLine();

            path = @"C:\Users\Michael.Felak\Desktop\result_1.csv";
            if (!string.IsNullOrEmpty(path))
            {
                using (var reader = new System.IO.StreamReader(path))
                {
                    CopyServerEventsToStaging(new CsvReader(reader), connection);
                }
            }
        }

        private static void CopyServerEventsToStaging(CsvReader csv, SqlConnection connection)
        {
            var dt = CreateStagingDataTable();

            //remove header row
            csv.Read();

            while (csv.Read())
            {
                if (dt == null)
                {
                    dt = CreateStagingDataTable();
                }
                var se = new ServerEvent();
                //csv.GetRecords<ServerEvent>();
                var id = csv.GetField<string>(0);
                var pod = csv.GetField<string>(1);
                var server = csv.GetField<string>(2);
                var service = csv.GetField<string>(3);
                var serviceTypeId = csv.GetField<string>(4);
                var type = csv.GetField<string>(5);
                var date = csv.GetField<string>(6);
                var details = csv.GetField<string>(7);

                if (!string.IsNullOrEmpty(id + pod + server + service + serviceTypeId + type + date + details))
                {
                    dt.Rows.Add(id, pod, server, service, serviceTypeId, type, details, date);

                    System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                    sw.Reset();
                    sw.Start();

                    if (dt.Rows.Count == 1000)
                    {
                        try
                        {
                            WriteToServer(dt, connection);
                            dt.Rows.Clear();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                        }
                        finally
                        {
                            sw.Stop();
                            Console.WriteLine($"Finished in {sw.ElapsedMilliseconds} ms.");
                            dt.Dispose();
                        }
                    }
                }
            }

            WriteToServer(dt, connection);
        }

        private static DataTable CreateStagingDataTable()
        {
            var dt = new DataTable();
            dt.Columns.Add("id");
            dt.Columns.Add("pod");
            dt.Columns.Add("server");
            dt.Columns.Add("service");
            dt.Columns.Add("service_type_id");
            dt.Columns.Add("type");
            dt.Columns.Add("details");
            dt.Columns.Add("date");

            return dt;
        }

        private static void WriteToServer(DataTable dt, SqlConnection connection)
        {
            Console.Write($"Writing {dt.Rows.Count} rows to dbo.stg_server_events... ");
            var sbc = new SqlBulkCopy(connection);
            sbc.DestinationTableName = "dbo.stg_server_events";
            try
            {
                sbc.WriteToServer(dt);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                sbc.Close();
                sbc = null;
            }
        }

        private static void ClearStagingTable(SqlConnection connection)
        {
            var tsql = ClearStagingTableSql();
            Console.WriteLine($"===============================");
            Console.Write($"Clearing staging table...");

            if (!string.IsNullOrEmpty(tsql))
            {
                using (var command = new SqlCommand(tsql, connection))
                {
                    try
                    {
                        command.ExecuteNonQuery();
                        Console.WriteLine($"Done.");
                        Console.WriteLine($"===============================\n");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                }
            }
        }

        private static void SubmitServerEventsSQL(SqlConnection connection)
        {
            var overallCount = 0;

            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            foreach (var service in MaverickServices)
            {
                sw.Reset();
                sw.Start();
                var list = new List<ServerEvent>();
                string tsql = BuildServerEventsSQL(service);

                using (var command = new SqlCommand(tsql, connection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        if (log) { Console.WriteLine($"Reading events for {service}"); }
                        var count = 0;

                        while (reader.Read())
                        {
                            if (list == null)
                            {
                                list = new List<ServerEvent>();
                            }

                            count++;
                            overallCount++;
                            if (log) { Console.WriteLine($"Server event #{count}."); }
                            if (log) { Console.SetCursorPosition(0, Console.CursorTop - 1); }
                            var serverEvent = new ServerEvent();
                            serverEvent.Id = reader.GetString(0);
                            serverEvent.Pod = reader.GetString(1);
                            serverEvent.Server = reader.GetString(2);
                            serverEvent.Service = reader.GetString(3);
                            serverEvent.ServiceTypeId = reader.GetString(4);
                            serverEvent.Type = reader.GetString(5);
                            serverEvent.Details = reader.GetString(6);
                            serverEvent.Date = GetDateTimeOffset(reader.GetString(7));
                            serverEvent.Url = GetUrlFromDetails(serverEvent.Details);
                            serverEvent.TenantId = GetTenantId(serverEvent.Details);
                            serverEvent.AuthenticationUserId = GetAuthUserId(serverEvent.Details);
                            serverEvent.EventMessage = GetEventMessage(serverEvent.Details);
                            serverEvent.Route = GetRouteFromUrl(serverEvent.Url);
                            serverEvent.IsTimeout = GetTimeout(serverEvent.Details);
                            serverEvent.IsUnhandledException = GetUnhandledException(serverEvent.Details);
                            serverEvent.IsFailedMonitor = GetFailedMonitor(serverEvent.Details);
                            serverEvent.IsHangfire = GetHangfire(serverEvent.Details);
                            serverEvent.HttpStatusCode = GetHttpStatusCode(serverEvent.Details);

                            list.Add(serverEvent);

                            //if(list.Count == 1000)
                            //{
                            //    ExtractDetailsFromServerEvents(list, connection);
                            //    sw.Stop();
                            //    Console.WriteLine($"{list.Count} record details extracted for {service} in {sw.ElapsedMilliseconds} ms.");
                            //    list = null;
                            //}
                        }

                        var message = $"{count} total events for {service}";
                        Console.WriteLine($"----{message}----");
                        LogToFile(message);
                        Console.WriteLine();
                    }
                }

                if (list.Any())
                {
                    ExtractDetailsFromServerEvents(list, connection);
                    sw.Stop();
                    Console.WriteLine($"Finished in {sw.ElapsedMilliseconds} ms.");
                    list.Clear();
                }

                list = null;
            }

            var finalMessage = $"{overallCount} records processed across {MaverickServices.Count()} services.\n\n";
            LogToFile(finalMessage);
            Console.WriteLine(finalMessage);
        }

        private static void LogToFile(string message)
        {
            using (var s = new System.IO.StreamWriter(logFileName, true))
            {
                s.WriteLine(message);
            }
        }

        private static void ExtractDetailsFromServerEvents(IEnumerable<ServerEvent> events, SqlConnection connection)
        {
            var dt = new DataTable();

            dt.Columns.Add("id");
            dt.Columns.Add("pod");
            dt.Columns.Add("server");
            dt.Columns.Add("service");
            dt.Columns.Add("service_type_id");
            dt.Columns.Add("type");
            dt.Columns.Add("details");
            dt.Columns.Add("date", typeof(DateTimeOffset));
            dt.Columns.Add("url");
            dt.Columns.Add("tenant_id");
            dt.Columns.Add("authentication_user_id");
            dt.Columns.Add("event_message");
            dt.Columns.Add("route");
            dt.Columns.Add("is_timeout", typeof(bool));
            dt.Columns.Add("is_unhandled_exception", typeof(bool));
            dt.Columns.Add("is_failed_monitor", typeof(bool));
            dt.Columns.Add("is_hangfire", typeof(bool));
            dt.Columns.Add("http_status_code");

            foreach (var e in events)
            {
                dt.Rows.Add(e.Id,
                    e.Pod,
                    e.Server,
                    e.Service,
                    e.ServiceTypeId,
                    e.Type,
                    e.Details,
                    e.Date,
                    e.Url,
                    e.TenantId,
                    e.AuthenticationUserId,
                    e.EventMessage,
                    e.Route,
                    e.IsTimeout,
                    e.IsUnhandledException,
                    e.IsFailedMonitor,
                    e.IsHangfire,
                    e.HttpStatusCode);
            }

            var sw = new Stopwatch();
            sw.Reset();
            Console.Write($"Inserting {dt.Rows.Count} rows into dbo.server_events... ");
            sw.Start();
            var sbc = new SqlBulkCopy(connection);
            sbc.BatchSize = 1000;
            sbc.DestinationTableName = "dbo.server_events";
            dt.Columns.Cast<System.Data.DataColumn>().ToList().ForEach(x => sbc.ColumnMappings.Add(new SqlBulkCopyColumnMapping(x.ColumnName, x.ColumnName)));

            try
            {
                sbc.WriteToServer(dt);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                throw new Exception("Write failed.");
            }
            finally
            {
                sw.Stop();
                Console.WriteLine($"Finished in {sw.ElapsedMilliseconds} ms.");
            }
        }

        #region Details parsing methods

        private static DateTimeOffset? GetDateTimeOffset(string date)
        {
            DateTime dt;
            if (!string.IsNullOrEmpty(date))
            {
                if (DateTime.TryParse(date, out dt))
                {
                    return dt;
                }
            }

            return null;
        }

        private static string GetTenantId(string details)
        {
            string tenantId = string.Empty;

            foreach (var searchString in GetTenantIdStrings)
            {
                tenantId = GetTenantIdSubstring(searchString, details);

                if (!string.IsNullOrEmpty(tenantId) && ValidateGuid(tenantId))
                {
                    return tenantId;
                }
            }

            return null;
        }

        private static string GetTenantIdSubstring(string search, string details)
        {
            var tenantIdLength = 36;
            var index = details.IndexOf(search);
            if (index >= 0)
            {
                return details.Substring(index + search.Length, tenantIdLength);
            }

            return null;
        }

        private static bool ValidateGuid(string tenantId)
        {
            Guid tId;
            return Guid.TryParse(tenantId, out tId);
        }

        private static string GetEventMessage(string details)
        {
            var search = "Event message: ";
            var index = details.IndexOf(search);
            var end = details.IndexOf("Event time", index);
            if (index >= 0)
            {
                if (end > 0)
                {
                    return details.Substring(index + search.Length, end - 2 - index - search.Length);
                }

                end = details.IndexOf("Event details", index);
                if (end >= 0)
                {
                    return details.Substring(index + search.Length, end - 2 - index - search.Length);
                }
            }
            return null;
        }

        private static string GetAuthUserId(string details)
        {
            var tenantIdLength = 36;
            string tenantId = string.Empty;

            if (!string.IsNullOrEmpty(details))
            {
                var search = "The authentication user was '";
                var index = details.IndexOf(search);
                if (index >= 0)
                {
                    tenantId = details.Substring(index + search.Length, tenantIdLength);
                }
            }

            if (ValidateGuid(tenantId))
            {
                return tenantId;
            }

            return null;
        }

        private static string GetUrlFromDetails(string details)
        {
            var searchString = "http";
            var index = details.IndexOf(searchString);
            if (index >= 0)
            {
                var end = details.IndexOf(" ", index);
                var end2 = details.IndexOf("\n", index);

                if (end2 < end)
                {
                    return details.Substring(index, end2 - index).TrimEnd('.');
                }

                return details.Substring(index, end - index).TrimEnd('.');
            }
            return null;
        }

        private static string GetRouteFromUrl(string url)
        {
            var route = GetRouteFromUrl(url, ".com");
            if (string.IsNullOrEmpty(route))
            {
                return GetRouteFromUrl(url, ".net");
            }

            return route;
        }

        private static string GetRouteFromUrl(string url, string search)
        {
            if (string.IsNullOrEmpty(url))
            {
                return null;
            }

            var index = url.IndexOf(search);

            if (url.Length > index + 4)
            {
                //if port number is present
                if (url.Substring(index + 4, 1) == ":")
                {
                    index = url.IndexOf("/", index + 1);
                    search = string.Empty;
                }
            }

            //remove query string parameters
            var end = url.IndexOf("?");
            if (index >= 0)
            {
                if (end > 0)
                {
                    return RemoveGuidFromRoute(url.Substring(index + search.Length, end - index - search.Length));
                }

                return RemoveGuidFromRoute(url.Substring(index + search.Length));
            }



            return null;
        }

        private static string RemoveGuidFromRoute(string url)
        {
            var parts = url.Split('/');
            var sb = new System.Text.StringBuilder();

            var idPattern = @"/[0-9]+/";
            var guidPattern = @"^(\{){0,1}[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}(\}){0,1}$";

            foreach (var part in parts)
            {
                if (!string.IsNullOrEmpty(part))
                {
                    if (!System.Text.RegularExpressions.Regex.IsMatch(part, guidPattern) && !System.Text.RegularExpressions.Regex.IsMatch($"/{part}/", idPattern))
                    {
                        sb.Append("/");
                        sb.Append(part);
                    }
                    else
                    {
                        sb.Append("/");
                        sb.Append("{0}");
                    }
                }
            }

            if (!string.IsNullOrEmpty(sb.ToString()))
            {
                return sb.ToString().TrimEnd('.');
            }

            return null;
        }

        private static bool GetTimeout(string details)
        {
            return GetBooleanResult(details.ToLower(), "timeout expired".ToLower()) || GetBooleanResult(details.ToLower(), "timeout".ToLower());
        }

        private static bool GetUnhandledException(string details)
        {
            return GetBooleanResult(details, "An unhandled exception");
        }

        private static bool GetHangfire(string details)
        {
            return GetBooleanResult(details, "hangfire");
        }

        private static bool GetFailedMonitor(string details)
        {
            return GetBooleanResult(details, "A service status monitor test failed.");
        }

        private static string GetHttpStatusCode(string details)
        {
            var search = "HTTP status code ";
            var index = details.IndexOf(search);
            if (index >= 0)
            {
                return details.Substring(index + search.Length, 3);
            }

            return null;
        }

        private static bool GetBooleanResult(string details, string search)
        {
            if (details.IndexOf(search) > 0)
            {
                return true;
            }

            return false;
        }

        #endregion Details parsing methods

        #region SQL

        private static string ClearStagingTableSql()
        {
            return "TRUNCATE TABLE stg_server_events";
        }

        private static string BuildServerEventsSQL(string service)
        {
            return string.Format(@"
                SELECT
                    stg.id,
                    stg.pod,
                    stg.server,
                    stg.service,
                    stg.service_type_id,
                    stg.type,
                    stg.details,
                    stg.date
                FROM 
                    stg_server_events stg
                LEFT JOIN 
                    server_events se ON se.id = stg.id
                WHERE 
                    se.id IS NULL 
                    and stg.service = '{0}'
                ", service);
        }

        #endregion SQL 

        private static SqlConnectionStringBuilder GetConnectionString()
        {
            var cb = new SqlConnectionStringBuilder();
            cb.DataSource = "mfelak.database.windows.net";
            cb.UserID = "RExAdmin";
            cb.Password = "PTL@dm1n";
            cb.InitialCatalog = "MaverickServerEvents";

            return cb;
        }

        #endregion Private
    }
}
