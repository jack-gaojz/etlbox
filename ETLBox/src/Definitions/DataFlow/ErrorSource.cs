using Newtonsoft.Json;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public class ErrorSource : DataFlowSource<ETLBoxError>
    {
        public ErrorSource()
        {

        }

        internal override void LinkBuffers(DataFlowTask successor, LinkPredicates linkPredicate)
        {
            var s = successor as IDataFlowDestination<ETLBoxError>;
            var lp = new BufferLinker<ETLBoxError>(linkPredicate);
            lp.LinkBlocksWithPredicates(SourceBlock, s.TargetBlock);
        }

        public void Send(Exception e, string jsonRow)
        {
            Buffer.SendAsync(new ETLBoxError()
            {
                ExceptionType = e.GetType().ToString(),
                ErrorText = e.Message,
                ReportTime = DateTime.Now,
                RecordAsJson = jsonRow
            }).Wait();
        }

        public static string ConvertErrorData<T>(T row)
        {
            try
            {
                return JsonConvert.SerializeObject(row, new JsonSerializerSettings());
            }
            catch (Exception e)
            {
                return e.Message;
            }
        }
    }
}
