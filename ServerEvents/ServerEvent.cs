namespace ServerEvents
{
    public class ServerEvent
    {
        public string Id { get; set; }
        public string Pod { get; set; }
        public string Server { get; set; }
        public string Service { get; set; }
        public string ServiceTypeId { get; set; }
        public string Type { get; set; }
        public System.DateTimeOffset? Date { get; set; }
        public string Details { get; set; }
        public string TenantId { get; set; }
        public string Url { get; set; }
        public string AuthenticationUserId { get; set; }
        public string EventMessage { get; set; }
        public string Route { get; set; }
        public bool IsTimeout { get; set; }
        public bool IsUnhandledException { get; set; }
        public bool IsFailedMonitor { get; set; }
        public bool IsHangfire { get; set; }

        public string HttpStatusCode { get; set; }
    }
}
