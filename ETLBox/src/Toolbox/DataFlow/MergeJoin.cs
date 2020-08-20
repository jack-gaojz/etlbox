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
        public Func<TInput1, TInput2, bool> BothMatchFunc { get; set; }

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
            Task.WaitAll(LeftJoinTarget.Completion, RightJoinTarget.Completion);
            EmptyQueues();
            Buffer.Complete();


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

        private Queue<TInput1> dataLeft = new Queue<TInput1>();
        private Queue<TInput2> dataRight = new Queue<TInput2>();
        private TInput1 IncomingLeft = default;
        private TInput2 IncomingRight = default;
        private TOutput joinOutput = default;

        private void EmptyQueues()
        {
            lock (joinLock)
            {
                while (dataLeft.Count > 0 || dataRight.Count > 0)
                {
                    TInput1 left = default;
                    TInput2 right = default;
                    if (dataLeft.Count > 0)
                        left = dataLeft.Dequeue();
                    if (dataRight.Count > 0)
                        right = dataRight.Dequeue();
                    CreateOutput(left, right);
                }
            }
        }
        public void LeftJoinData(TInput1 data)
        {
            lock (joinLock)
            {
                if (dataRight.Count >= 1)
                {
                    var right = dataRight.Dequeue();
                    CreateOutput(data, right);
                }
                else
                {
                    dataLeft.Enqueue(data);
                }
            }
        }

        private void CreateOutput(TInput1 dataLeft, TInput2 dataRight)
        {

            //if (BothMatchFunc == null)
            joinOutput = MergeJoinFunc.Invoke(dataLeft, dataRight);
            if (!Buffer.SendAsync(joinOutput).Result)
                throw Exception;
        }

        public void RightJoinData(TInput2 data)
        {
            lock (joinLock)
            {
                if (dataLeft.Count >= 1)
                {
                    var left = dataLeft.Dequeue();
                    CreateOutput(left, data);
                }
                else
                {
                    dataRight.Enqueue(data);
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

