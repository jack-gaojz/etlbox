using System.Collections.Generic;
using System.Threading.Tasks;

namespace ETLBox.DataFlow
{
    public interface IDataFlowComponent
    {
        List<IDataFlowComponent> Predecessors { get; }
        List<IDataFlowComponent> Sucessors { get; }
        Task Completion { get;}
    }
}
