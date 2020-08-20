using ETLBox.ControlFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// Will cross join data from the two inputs into one output. The input for the first table will be loaded into memory before the actual
    /// join can start. After this, every incoming row will be joined with every row of the InMemory-Table by the given function CrossJoinFunc.
    /// The InMemory target should always have the smaller amount of data to reduce memory consumption and processing time.
    /// </summary>
    /// <typeparam name="TInput1">Type of data for in memory input block.</typeparam>
    /// <typeparam name="TInput2">Type of data for processing input block.</typeparam>
    /// <typeparam name="TOutput">Type of output data.</typeparam>
    public class CrossJoin<TInput1, TInput2, TOutput> : DataFlowSource<TOutput>, IDataFlowTransformation<TOutput>
    {
        #region Public properties

        public override string TaskName { get; set; } = "Cross join data";
        public InMemoryDestination<TInput1> InMemoryTarget { get; set; }
        public ActionJoinTarget<TInput2> PassingTarget { get; set; }
        public Func<TInput1, TInput2, TOutput> CrossJoinFunc { get; set; }
        public override ISourceBlock<TOutput> SourceBlock => this.Buffer;

        #endregion

        #region Join Targets

        public class InMemoryDestination<TInput> : DataFlowJoinTarget<TInput>
        {
            public override ITargetBlock<TInput> TargetBlock => InMemoryTarget.TargetBlock;
            public MemoryDestination<TInput> InMemoryTarget { get; set; }

            public InMemoryDestination(DataFlowTask parent)
            {
                InMemoryTarget = new MemoryDestination<TInput>();
                CreateLinkInInternalFlow(parent);
            }

            protected override void InternalInitBufferObjects()
            {
                InMemoryTarget.CopyTaskProperties(Parent);
                InMemoryTarget.MaxBufferSize = -1;
                InMemoryTarget.InitBufferObjects();
            }

            protected override void CleanUpOnSuccess() { }

            protected override void CleanUpOnFaulted(Exception e) { }
        }

        #endregion

        #region Constructors

        public CrossJoin()
        {
            InMemoryTarget = new InMemoryDestination<TInput1>(this);
            PassingTarget = new ActionJoinTarget<TInput2>(this, CrossJoinData);
        }

        public CrossJoin(Func<TInput1, TInput2, TOutput> crossJoinFunc) : this()
        {
            CrossJoinFunc = crossJoinFunc;
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
            InMemoryTarget.CompleteBufferOnPredecessorCompletion();
            PassingTarget.CompleteBufferOnPredecessorCompletion();
            Task.WaitAll(InMemoryTarget.Completion, PassingTarget.Completion);
            Buffer.Complete();

        }

        internal override void FaultBufferOnPredecessorCompletion(Exception e)
        {
            InMemoryTarget.FaultBufferOnPredecessorCompletion(e);
            PassingTarget.FaultBufferOnPredecessorCompletion(e);
            ((IDataflowBlock)Buffer).Fault(e);
        }

        #endregion

        #region Implementation

        bool WasInMemoryTableLoaded;
        IEnumerable<TInput1> InMemoryData => InMemoryTarget.InMemoryTarget.Data;

        public void CrossJoinData(TInput2 passingRow)
        {
            NLogStartOnce();
            if (!WasInMemoryTableLoaded)
            {
                InMemoryTarget.Completion.Wait();
                WasInMemoryTableLoaded = true;
            }
            foreach (TInput1 inMemoryRow in InMemoryData)
            {
                try
                {
                    if (inMemoryRow != null && passingRow != null)
                    {
                        TOutput result = CrossJoinFunc.Invoke(inMemoryRow, passingRow);
                        if (result != null)
                        {
                            if (!Buffer.SendAsync(result).Result)
                                throw new ETLBoxException("Buffer already completed or faulted!", this.Exception);
                            LogProgress();
                        }
                    }
                }
                catch (ETLBoxException) { throw; }
                catch (Exception e)
                {
                    ThrowOrRedirectError(e, string.Concat(ErrorHandler.ConvertErrorData<TInput1>(inMemoryRow), "  |--| ",
                        ErrorHandler.ConvertErrorData<TInput2>(passingRow)));
                    LogProgress();
                }
            }
        }

        #endregion
    }

    /// <summary>
    /// Will cross join data from the two inputs into one output. The input for the first table will be loaded into memory before the actual
    /// join can start. After this, every incoming row will be joined with every row of the InMemory-Table by the given function CrossJoinFunc.
    /// The InMemory target should always have the smaller amount of data to reduce memory consumption and processing time.
    /// </summary>
    /// <typeparam name="TInput">Type of data for both inputs and output.</typeparam>
    public class CrossJoin<TInput> : CrossJoin<TInput, TInput, TInput>
    {
        public CrossJoin() : base()
        { }

        public CrossJoin(Func<TInput, TInput, TInput> crossJoinFunc) : base(crossJoinFunc)
        { }
    }

    /// <summary>
    /// Will cross join data from the two inputs into one output. The input for the first table will be loaded into memory before the actual
    /// join can start. After this, every incoming row will be joined with every row of the InMemory-Table by the given function CrossJoinFunc.
    /// The InMemory target should always have the smaller amount of data to reduce memory consumption and processing time.
    /// The non generic implementation deals with a dynamic object for both inputs and output.
    /// </summary>
    public class CrossJoin : CrossJoin<ExpandoObject, ExpandoObject, ExpandoObject>
    {
        public CrossJoin() : base()
        { }

        public CrossJoin(Func<ExpandoObject, ExpandoObject, ExpandoObject> crossJoinFunc) : base(crossJoinFunc)
        { }
    }
}

