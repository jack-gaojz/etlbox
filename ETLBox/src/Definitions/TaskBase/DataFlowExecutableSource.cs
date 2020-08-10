using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using NLog.Targets;
using System;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowExecutableSource<TOutput> : DataFlowSource<TOutput>, IDataFlowExecutableSource<TOutput>
    {
        #region Execution and IDataFlowExecutableSource
        public virtual void Execute() //remove virtual
        {
            InitNetworkRecursively();
            OnExecutionDoSynchronousWork();
            Completion.RunSynchronously();
        }

        public Task ExecuteAsync()
        {
            InitNetworkRecursively();
            OnExecutionDoSynchronousWork();
            Completion.Start();
            return Completion;
        }

        protected virtual void OnExecutionDoSynchronousWork() { } //abstract       
       

        #endregion
    }
}
