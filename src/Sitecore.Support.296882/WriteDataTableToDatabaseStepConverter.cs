using Sitecore.DataExchange.Attributes;
using Sitecore.DataExchange.Models;
using Sitecore.DataExchange.Repositories;
using Sitecore.Services.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Sitecore.Support.DataExchange.Providers.Sql.BatchHandling
{
  [SupportedIds("{06AFFE27-FEBB-4FD5-8966-5FC19567C7B8}")]
  public class WriteDataTableToDatabaseStepConverter : Sitecore.DataExchange.Providers.Sql.BatchHandling.WriteDataTableToDatabaseStepConverter
  {
    public WriteDataTableToDatabaseStepConverter(IItemModelRepository repository) : base(repository)
    {
    }
    public const string FieldNameBulkCopyTimeout = "BulkCopyTimeout";
    protected override void AddPlugins(ItemModel source, PipelineStep pipelineStep)
    {
      base.AddPlugins(source, pipelineStep);
      int sqlBulkCopyTimeout = this.GetIntValue(source, FieldNameBulkCopyTimeout);
      if (sqlBulkCopyTimeout == 0)
      {
        sqlBulkCopyTimeout = 30;
      }
      var sqlBulkCopysettings = new SqlBulkCopySettings()
      {
        SqlBulkCopyTimeout = sqlBulkCopyTimeout
      };
      pipelineStep.AddPlugin<SqlBulkCopySettings>(sqlBulkCopysettings);
    }
  }
}