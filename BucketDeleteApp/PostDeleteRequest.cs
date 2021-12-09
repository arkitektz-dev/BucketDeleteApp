using System;
using System.Collections.Generic;
using System.Text;

namespace BucketDeleteApp
{
    internal class PostDeleteRequest
    {
        public string ClientCode { get; set; }
        public string Program { get; set; }
        public string Description1 { get; set; }
        public string Description2 { get; set; }
        public string Username { get; set; }
    }
}
