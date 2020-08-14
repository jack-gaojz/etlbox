using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowDestination<TInput> : DataFlowTask, IDataFlowDestination<TInput>
    {
        #region Public properties

        public ITargetBlock<TInput> TargetBlock => TargetAction;

        public void Wait() => Completion.Wait();

        #endregion

        #region Buffer handling
        protected virtual ActionBlock<TInput> TargetAction { get; set; }

        internal override Task BufferCompletion => TargetBlock.Completion;

        internal override void CompleteBufferOnPredecessorCompletion() => TargetBlock.Complete();

        internal override void FaultBufferOnPredecessorCompletion(Exception e) => TargetBlock.Fault(e);

        #endregion

        public IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
            => InternalLinkErrorTo(target);
    }
}
