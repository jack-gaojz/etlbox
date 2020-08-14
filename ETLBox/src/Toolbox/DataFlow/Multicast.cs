using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks.Dataflow;


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
        public override ISourceBlock<TInput> SourceBlock => BroadcastBlock;
        public override ITargetBlock<TInput> TargetBlock => BroadcastBlock;

        #endregion

        #region Constructors

        public Multicast()
        {
            TypeInfo = new TypeInfo(typeof(TInput)).GatherTypeInfo();
            ObjectCopy = new ObjectCopy<TInput>(TypeInfo);
        }

        #endregion

        #region Implement abstract methods

        public override void InitBufferObjects()
        {
            BroadcastBlock = new BroadcastBlock<TInput>(Clone, new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
            WereBufferInitialized = true;
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        #endregion

        #region Implementation

        BroadcastBlock<TInput> BroadcastBlock;
        TypeInfo TypeInfo;
        ObjectCopy<TInput> ObjectCopy;

        private TInput Clone(TInput row)
        {
            NLogStartOnce();
            TInput clone = ObjectCopy.Clone(row);
            LogProgress();
            return clone;
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
