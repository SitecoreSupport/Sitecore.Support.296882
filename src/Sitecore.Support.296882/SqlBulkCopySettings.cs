using Sitecore.DataExchange;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Sitecore.Support
{
  public class SqlBulkCopySettings : IPlugin
  {
    public int SqlBulkCopyTimeout
    {
      get;
      set;
    }
  }
}