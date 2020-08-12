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
        //{
        //    get { return _mergeJoinFunc; }
        //    set
        //    {
        //        _mergeJoinFunc = value;
        //        Transformation.TransformationFunc = new Func<Tuple<TInput1, TInput2>, TOutput>(tuple => _mergeJoinFunc.Invoke(tuple.Item1, tuple.Item2));
        //        JoinBlock.LinkTo(Transformation.TargetBlock, new DataflowLinkOptions { PropagateCompletion = true });
        //    }
        //}

        //private Func<TInput1, TInput2, TOutput> _mergeJoinFunc;

        #endregion


        //public class JoinTarget<TInput> : DataFlowJoinTarget<TInput>
        //{
        //    public override ITargetBlock<TInput> TargetBlock => JoinAction;
        //    ActionBlock<TInput> JoinAction;
        //    Action<TInput> Action;
        //    public JoinTarget(DataFlowTask parent, Action<TInput> action)
        //    {
        //        Action = action;
        //        CreateLinkInInternalFlow(parent);
        //    }

        //    public override void InitBufferObjects()
        //    {
        //        JoinAction = new ActionBlock<TInput>(Action, new ExecutionDataflowBlockOptions()
        //        {
        //            BoundedCapacity = MaxBufferSize
        //        });
        //    }

        //    protected override void CleanUpOnSuccess() { }

        //    protected override void CleanUpOnFaulted(Exception e) { }
        //}




        internal JoinBlock<TInput1, TInput2> JoinBlock { get; set; }
        internal RowTransformation<Tuple<TInput1, TInput2>, TOutput> Transformation { get; set; }

        #region Constructors

        public MergeJoin()
        {
            LeftJoinTarget = new ActionJoinTarget<TInput1>(this, LeftJoinData);
            RightJoinTarget = new ActionJoinTarget<TInput2>(this, RightJoinData);
            //Target1 = new MergeJoinTarget<TInput1>(this, JoinBlock.Target1);
            //Target2 = new MergeJoinTarget<TInput2>(this, JoinBlock.Target2);
        }

        public MergeJoin(Func<TInput1, TInput2, TOutput> mergeJoinFunc) : this()
        {
            MergeJoinFunc = mergeJoinFunc;
        }


        #endregion

        #region Implement abstract methods and override default behaviour

        public override void InitBufferObjects()
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
        protected override Task BufferCompletion => SourceBlock.Completion;

        internal override void CompleteBuffer()
        {
            LeftJoinTarget.TargetBlock.Complete();
            RightJoinTarget.TargetBlock.Complete();
            Buffer.Complete();
        }

        internal override void FaultBuffer(Exception e)
        {
            LeftJoinTarget.TargetBlock.Fault(e);
            RightJoinTarget.TargetBlock.Fault(e);
            ((IDataflowBlock)Buffer).Fault(e);
        }

        #endregion
        //public override void InitBufferObjects()
        //{
        //    Transformation = new RowTransformation<Tuple<TInput1, TInput2>, TOutput>();
        //    Transformation.CopyTaskProperties(this);
        //    if (MaxBufferSize > 0) Transformation.MaxBufferSize = this.MaxBufferSize;
        //    JoinBlock = new JoinBlock<TInput1, TInput2>(new GroupingDataflowBlockOptions()
        //    {
        //        BoundedCapacity = MaxBufferSize
        //    });
        //}

        #region Implementation

        public void LeftJoinData(TInput1 data)
        {

        }
        public void RightJoinData(TInput2 data)
        {
        }

        #endregion

        //public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target)
        //    => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target);

        //public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target, Predicate<TOutput> predicate)
        //    => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target, predicate);

        //public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
        //    => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target, rowsToKeep, rowsIntoVoid);

        //public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target)
        //    => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target);

        //public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target, Predicate<TOutput> predicate)
        //    => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target, predicate);

        //public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
        //    => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target, rowsToKeep, rowsIntoVoid);

        //public void LinkErrorTo(IDataFlowDestination<ETLBoxError> target) =>
        //    Transformation.LinkErrorTo(target);
    }

    //public class MergeJoinTarget<TInput> : DataFlowTask, IDataFlowDestination<TInput>
    //{
    //    public ITargetBlock<TInput> TargetBlock { get; set; }

    //    public void Wait()
    //    {
    //        TargetBlock.Completion.Wait();
    //    }

    //    public Task Completion => TargetBlock.Completion;

    //    protected List<Task> PredecessorCompletions { get; set; } = new List<Task>();

    //    //TODO
    //    public List<DataFlowTask> Predecessors { get; set; }

    //    public void AddPredecessorCompletion(Task completion)
    //    {
    //        PredecessorCompletions.Add(completion);
    //        completion.ContinueWith(t => CheckCompleteAction());
    //    }

    //    protected void CheckCompleteAction()
    //    {
    //        Task.WhenAll(PredecessorCompletions).ContinueWith(t =>
    //        {
    //            if (t.IsFaulted) TargetBlock.Fault(t.Exception.InnerException);
    //            else TargetBlock.Complete();
    //        });
    //    }

    //    public MergeJoinTarget(ITask parent, ITargetBlock<TInput> joinTarget)
    //    {
    //        TargetBlock = joinTarget;
    //        CopyTaskProperties(parent);

    //    }
    //}

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

