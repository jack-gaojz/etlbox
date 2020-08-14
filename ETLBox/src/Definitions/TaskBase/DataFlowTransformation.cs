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

        internal override Task BufferCompletion => SourceBlock.Completion;

        internal override void CompleteBufferOnPredecessorCompletion()
        {
            if (TargetBlock != SourceBlock)
                throw new NotImplementedException("Component must override this method!");
            else
                TargetBlock.Complete();
        }



        internal override void FaultBufferOnPredecessorCompletion(Exception e) {
            if (TargetBlock != SourceBlock)
                throw new NotImplementedException("Component must override this method!");
            else
                TargetBlock.Fault(e);
        }

    }
}
