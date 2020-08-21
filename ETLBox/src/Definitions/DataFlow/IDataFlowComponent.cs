using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ETLBox.DataFlow
{
    public interface IDataFlowComponent
    {
        Task Completion { get;}
        Action OnCompletion { get; set; }
        Exception Exception { get; }
        int MaxBufferSize { get; set; }
    }
}
