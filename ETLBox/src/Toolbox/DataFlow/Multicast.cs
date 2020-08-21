using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Transactions;

namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// A multicast duplicates data from the input into two outputs.
    /// </summary>
    /// <typeparam name="TInput">Type of input data.</typeparam>
    /// <example>
    /// <code>
    /// Multicast&lt;MyDataRow&gt; multicast = new Multicast&lt;MyDataRow&gt;();
    /// multicast.LinkTo(dest1);
    /// multicast.LinkTo(dest2);
    /// </code>
    /// </example>
    public class Multicast<TInput> : DataFlowTransformation<TInput, TInput>
    {
        #region Public properties

        public override string TaskName { get; set; } = "Multicast - duplicate data";
        public override ISourceBlock<TInput> SourceBlock
        {
            get
            {
                if (AvoidBroadcastBlock)
                    return OutputBuffer?.LastOrDefault();
                else
                    return BroadcastBlock;
            }
        }

        public override ITargetBlock<TInput> TargetBlock =>
            AvoidBroadcastBlock ? (ITargetBlock<TInput>)OwnBroadcastBlock : BroadcastBlock;

        #endregion

        #region Constructors

        public Multicast()
        {
            TypeInfo = new TypeInfo(typeof(TInput)).GatherTypeInfo();
            ObjectCopy = new ObjectCopy<TInput>(TypeInfo);
        }

        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {
            if (Successors.Any(suc => suc.MaxBufferSize > 0))
            {
                AvoidBroadcastBlock = true;
                OwnBroadcastBlock = new ActionBlock<TInput>(Broadcast, new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = MaxBufferSize
                });
            }
            else
            {
                BroadcastBlock = new BroadcastBlock<TInput>(Clone, new DataflowBlockOptions()
                {
                    BoundedCapacity = MaxBufferSize
                });
            }
        }

        internal override void LinkBuffers(DataFlowTask successor, LinkPredicates linkPredicates)
        {
            if (AvoidBroadcastBlock)
            {
                var buffer = new BufferBlock<TInput>(new DataflowBlockOptions()
                {
                    BoundedCapacity = MaxBufferSize
                });
                OutputBuffer.Add(buffer);
            }
            base.LinkBuffers(successor, linkPredicates);
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        internal override Task BufferCompletion
        {
            get
            {
                if (AvoidBroadcastBlock)
                    return Task.WhenAll(OutputBuffer.Select(b => b.Completion));
                else
                   return ((IDataflowBlock)BroadcastBlock).Completion;
            }
        }

        internal override void CompleteBufferOnPredecessorCompletion()
        {
            if (AvoidBroadcastBlock)
            {
                OwnBroadcastBlock.Complete();
                OwnBroadcastBlock.Completion.Wait();
                foreach (var buffer in OutputBuffer)
                    buffer.Complete();
            }
            else
            {
                BroadcastBlock.Complete();
            }
        }

        internal override void FaultBufferOnPredecessorCompletion(Exception e)
        {
            if (AvoidBroadcastBlock)
            {
                ((IDataflowBlock)OwnBroadcastBlock).Fault(e);
                OwnBroadcastBlock.Completion.Wait();
                foreach (var buffer in OutputBuffer)
                    ((IDataflowBlock)buffer).Fault(e);
            }
            else
            {
                ((IDataflowBlock)BroadcastBlock).Fault(e);
            }
        }

        #endregion

        #region Implementation
        bool AvoidBroadcastBlock;
        BroadcastBlock<TInput> BroadcastBlock;
        ActionBlock<TInput> OwnBroadcastBlock;
        List<BufferBlock<TInput>> OutputBuffer = new List<BufferBlock<TInput>>();
        TypeInfo TypeInfo;
        ObjectCopy<TInput> ObjectCopy;

        private TInput Clone(TInput row)
        {
            NLogStartOnce();
            TInput clone = ObjectCopy.Clone(row);
            LogProgress();
            return clone;
        }

        private void Broadcast(TInput row)
        {
            TInput clone = Clone(row);
            foreach (var buffer in OutputBuffer)
            {
                if (!buffer.SendAsync(clone).Result)
                    throw new ETLBoxException("Buffer already completed or faulted!", this.Exception);
            }
        }

        #endregion
    }

    /// <summary>
    /// A multicast duplicates data from the input into two outputs. The non generic version or the multicast
    /// excepct a dynamic object as input and has two output with the copies of the input.
    /// </summary>
    /// <see cref="Multicast{TInput}"></see>
    /// <example>
    /// <code>
    /// //Non generic Multicast works with dynamic object as input and output
    /// Multicast multicast = new Multicast();
    /// multicast.LinkTo(dest1);
    /// multicast.LinkTo(dest2);
    /// </code>
    /// </example>
    public class Multicast : Multicast<ExpandoObject>
    {
        public Multicast() : base() { }
    }
}
