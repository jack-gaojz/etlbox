using ETLBox.ControlFlow;
using ETLBox.DataFlow.Connectors;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Runtime.InteropServices.ComTypes;
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
    public class CrossJoin<TInput1, TInput2, TOutput> : DataFlowSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        #region Public properties
        public override string TaskName { get; set; } = "Cross join data";
        public MemoryDestination<TInput1> InMemoryTarget { get; set; }
        public CustomDestination<TInput2> PassingTarget { get; set; }
        public Func<TInput1, TInput2, TOutput> CrossJoinFunc { get; set; }

        #endregion

        #region Constructors

        public CrossJoin()
        {
            InMemoryTarget = new MemoryDestination<TInput1>();
            PassingTarget = new CustomDestination<TInput2>(CrossJoinData);
            PassingTarget.OnBeforeInit = Init;
            PassingTarget.OnCompletion =
                () => Completion.RunSynchronously();
        }

        public CrossJoin(Func<TInput1, TInput2, TOutput> crossJoinFunc) : this()
        {
            CrossJoinFunc = crossJoinFunc;
        }

        #endregion

        #region Implement abstract methods

        void Init()
        {
            InitNetworkRecursively();
            InMemoryTarget.CopyTaskProperties(this);
            PassingTarget.CopyTaskProperties(this);
            if (MaxBufferSize > 0) PassingTarget.MaxBufferSize = this.MaxBufferSize;
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }


        protected override void CleanUpOnFaulted(Exception e) { }

        protected override void OnExecutionDoSynchronousWork() { }

        protected override void OnExecutionDoAsyncWork()
        {
            ;
        }

        #endregion

        #region Implementation

        bool WasInMemoryTableLoaded;
        IEnumerable<TInput1> InMemoryData => InMemoryTarget.Data;

        private void CrossJoinData(TInput2 passingRow)
        {
            NLogStartOnce();
            if (!WasInMemoryTableLoaded)
            {
                InMemoryTarget.Wait();
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
                                throw Exception;
                            LogProgress();
                        }
                    }
                }
                catch (Exception e)
                {
                    ThrowOrRedirectError(e, string.Concat(ErrorHandler.ConvertErrorData<TInput1>(inMemoryRow), "  |--| ",
                        ErrorHandler.ConvertErrorData<TInput2>(passingRow)));
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

