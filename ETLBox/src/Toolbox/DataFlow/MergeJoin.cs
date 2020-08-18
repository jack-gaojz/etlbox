using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// Will join data from the two inputs into one output - on a row by row base. Make sure both inputs are sorted or in the right order.
    /// </summary>
    /// <typeparam name="TInput1">Type of data for input block one.</typeparam>
    /// <typeparam name="TInput2">Type of data for input block two.</typeparam>
    /// <typeparam name="TOutput">Type of output data.</typeparam>
    /// <example>
    /// <code>
    /// MergeJoin&lt;MyDataRow1, MyDataRow2, MyDataRow1&gt; join = new MergeJoin&lt;MyDataRow1, MyDataRow2, MyDataRow1&gt;(Func&lt;TInput1, TInput2, TOutput&gt; mergeJoinFunc);
    /// source1.LinkTo(join.Target1);;
    /// source2.LinkTo(join.Target2);;
    /// join.LinkTo(dest);
    /// </code>
    /// </example>
    public class MergeJoin<TInput1, TInput2, TOutput> : DataFlowSource<TOutput>, IDataFlowTransformation<TOutput>
    {


        #region Public properties

        public override string TaskName { get; set; } = "Merge and join data";
        public ActionJoinTarget<TInput1> LeftJoinTarget { get; set; }
        public ActionJoinTarget<TInput2> RightJoinTarget { get; set; }
        //public ISourceBlock<TOutput> SourceBlock => Transformation.SourceBlock;
        public override ISourceBlock<TOutput> SourceBlock => this.Buffer;
        public Func<TInput1, TInput2, TOutput> MergeJoinFunc { get; set; }
        public Func<TInput1,TInput2, bool> BothMatchFunc { get; set; }

        #endregion


        #region Constructors

        public MergeJoin()
        {
            LeftJoinTarget = new ActionJoinTarget<TInput1>(this, LeftJoinData);
            RightJoinTarget = new ActionJoinTarget<TInput2>(this, RightJoinData);
        }

        public MergeJoin(Func<TInput1, TInput2, TOutput> mergeJoinFunc) : this()
        {
            MergeJoinFunc = mergeJoinFunc;
        }


        #endregion

        #region Implement abstract methods and override default behaviour

        protected override void InternalInitBufferObjects()
        {
            Buffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }


        protected override void CleanUpOnFaulted(Exception e) { }

        protected BufferBlock<TOutput> Buffer { get; set; }
        internal override Task BufferCompletion => SourceBlock.Completion;

        internal override void CompleteBufferOnPredecessorCompletion()
        {
            LeftJoinTarget.CompleteBufferOnPredecessorCompletion();
            RightJoinTarget.CompleteBufferOnPredecessorCompletion();
            Task.WhenAll(LeftJoinTarget.Completion, RightJoinTarget.Completion).ContinueWith(
                t => Buffer.Complete()
            );
        }

        internal override void FaultBufferOnPredecessorCompletion(Exception e)
        {
            LeftJoinTarget.FaultBufferOnPredecessorCompletion(e);
            RightJoinTarget.FaultBufferOnPredecessorCompletion(e);
            ((IDataflowBlock)Buffer).Fault(e);
        }

        #endregion

        #region Implementation

        private readonly object joinLock = new object();

        private int left;
        private int right;

        private TInput1 dataLeft = default;
        private TInput2 dataRight = default;
        private TInput1 IncomingLeft = default;
        private TInput2 IncomingRight = default;
        private TOutput joinOutput = default;

        public void LeftJoinData(TInput1 data)
        {
            lock (joinLock)
            {
                IncomingLeft = data;
                if (left <= right)
                {
                    dataLeft = data;
                    left++;
                    ControlFlow.ControlFlow.STAGE = $"Left {left}";
                    LogProgress();
                    if (left == right)
                        CreateOutput();
                }
            }
        }

        private void CreateOutput()
        {

                if (BothMatchFunc == null)
                    joinOutput = MergeJoinFunc.Invoke(dataLeft, dataRight);
                if (!Buffer.SendAsync(joinOutput).Result)
                    throw Exception;
        }

        public void RightJoinData(TInput2 data)
        {
            lock (joinLock)
            {
                IncomingRight = data;
                if (right <= left)
                {
                    dataRight = data;
                    right++;
                    ControlFlow.ControlFlow.STAGE = $"Right {right}";
                    LogProgress();
                    if (right == left)
                        CreateOutput();
                }
            }
        }

        #endregion

    }

    /// <summary>
    /// Will join data from the two inputs into one output - on a row by row base. Make sure both inputs are sorted or in the right order.
    /// </summary>
    /// <typeparam name="TInput">Type of data for both inputs and output.</typeparam>
    /// <example>
    /// <code>
    /// MergeJoin&lt;MyDataRow&gt; join = new MergeJoin&lt;MyDataRow&gt;(mergeJoinFunc);
    /// source1.LinkTo(join.Target1);;
    /// source2.LinkTo(join.Target2);;
    /// join.LinkTo(dest);
    /// </code>
    /// </example>
    public class MergeJoin<TInput> : MergeJoin<TInput, TInput, TInput>
    {
        public MergeJoin() : base()
        { }

        public MergeJoin(Func<TInput, TInput, TInput> mergeJoinFunc) : base(mergeJoinFunc)
        { }
    }

    /// <summary>
    /// Will join data from the two inputs into one output - on a row by row base.
    /// Make sure both inputs are sorted or in the right order. The non generic implementation deals with
    /// a dynamic object as input and merged output.
    /// </summary>
    public class MergeJoin : MergeJoin<ExpandoObject, ExpandoObject, ExpandoObject>
    {
        public MergeJoin() : base()
        { }

        public MergeJoin(Func<ExpandoObject, ExpandoObject, ExpandoObject> mergeJoinFunc) : base(mergeJoinFunc)
        { }
    }
}

