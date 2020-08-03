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

        protected override void LinkBuffers(DataFlowTask successor, LinkPredicate linkPredicate)
        {
            var s = successor as IDataFlowLinkTarget<ETLBoxError>;
            var lp = new Linker<ETLBoxError>(linkPredicate?.Predicate, linkPredicate?.VoidPredicate);
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
