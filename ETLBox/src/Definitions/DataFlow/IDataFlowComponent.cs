using System.Collections.Generic;
using System.Threading.Tasks;

namespace ETLBox.DataFlow
{
    public interface IDataFlowComponent
    {
        List<DataFlowTask> Predecessors { get; }
        List<DataFlowTask> Sucessors { get; }
        Task Completion { get;}
    }
}
