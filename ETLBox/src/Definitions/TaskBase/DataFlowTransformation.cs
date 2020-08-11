using ETLBox.ControlFlow;
using NLog.Targets;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowTransformation<TInput, TOutput> : DataFlowSource<TOutput>, IDataFlowTransformation<TInput, TOutput>
    {
        public virtual ITargetBlock<TInput> TargetBlock { get; } //abstract

        protected override Task BufferCompletion => SourceBlock.Completion;

        protected override void CompleteBuffer()=> TargetBlock.Complete();

        protected override void FaultBuffer(Exception e) => TargetBlock.Fault(e);

    }
}
