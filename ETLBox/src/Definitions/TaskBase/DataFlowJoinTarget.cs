using ETLBox.ControlFlow;
using NLog.Targets;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowJoinTarget<TInput> : DataFlowTask, IDataFlowDestination<TInput>
    {
        protected override Task BufferCompletion => TargetBlock.Completion;

        protected override void CompleteBuffer() => TargetBlock.Complete();

        protected override void FaultBuffer(Exception e) => TargetBlock.Fault(e);

        public virtual ITargetBlock<TInput> TargetBlock { get; }

        public void CreateLinkInInternalFlow(DataFlowTask parent)
        {
            Parent = parent;
            InternalLinkTo<TInput>(parent as IDataFlowDestination);
        }
    }
}
