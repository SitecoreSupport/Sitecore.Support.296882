using Sitecore.DataExchange.Contexts;
using Sitecore.DataExchange.Models;
using Sitecore.DataExchange.Providers.Sql.Endpoints;
using Sitecore.Services.Core.Diagnostics;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Web;


namespace Sitecore.Support.DataExchange.Providers.Sql.BatchHandling
{
  public class WriteDataTableToDatabaseStepProcessor : Sitecore.DataExchange.Providers.Sql.BatchHandling.WriteDataTableToDatabaseStepProcessor
  {
    protected override void ProcessPipelineStep(PipelineStep pipelineStep, PipelineContext pipelineContext, ILogger logger)
    {
      var tablePlugin = this.GetBatchStoragePlugin(pipelineStep, pipelineContext, logger);
      if (tablePlugin == null || tablePlugin.Batch == null)
      {
        return;
      }
      if (!ShouldWriteToPermanentStorage(tablePlugin.Batch.Rows.Count, tablePlugin, pipelineStep, pipelineContext, logger))
      {
        return;
      }
      var endpoint = this.GetEndpoint(pipelineStep, pipelineContext, logger);
      if (endpoint == null)
      {
        this.Log(logger.Error, pipelineContext, "No endpoint is set on the plugin.");
        pipelineContext.CriticalError = true;
        return;
      }
      var endpointSettings = endpoint.GetPlugin<DatabaseConnectionEndpointSettings>();
      if (string.IsNullOrWhiteSpace(endpointSettings.ConnectionString))
      {
        this.Log(logger.Error, pipelineContext, "No connection string is set on the endpoint.", $"endpoint: {endpoint.Name}");
        pipelineContext.CriticalError = true;
        return;
      }
      using (var connection = new SqlConnection(endpointSettings.ConnectionString))
      {
        connection.Open();
        using (var bulkCopy = new SqlBulkCopy(connection))
        {
          var sqlBulkCopySettings = pipelineStep.GetPlugin<SqlBulkCopySettings>();
          bulkCopy.BulkCopyTimeout = sqlBulkCopySettings.SqlBulkCopyTimeout;
          foreach (DataColumn c in tablePlugin.Batch.Columns)
          {
            bulkCopy.ColumnMappings.Add(c.ColumnName, c.ColumnName);
          }
          bulkCopy.DestinationTableName = tablePlugin.Batch.TableName;
          try
          {
            bulkCopy.WriteToServer(tablePlugin.Batch);
          }
          catch (Exception ex)
          {
            this.LogException(ex, logger.Error, pipelineContext, "Unable to bulk copy data from data table to database.");
            pipelineContext.CriticalError = true;
            return;
          }
          // MKL: We need to cleanup the table after submit.
          var newTable = new DataTable(tablePlugin.Batch.TableName);

          foreach (DataColumn batchColumn in tablePlugin.Batch.Columns)
          {
            newTable.Columns.Add(new DataColumn(batchColumn.ColumnName, batchColumn.DataType));
          }

          tablePlugin.Batch = newTable;
        }

      }
    }
  }
}