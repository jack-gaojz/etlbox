using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// Sort the input with the given sort function.
    /// </summary>
    /// <typeparam name="TInput">Type of input data (equal type of output data).</typeparam>
    /// <example>
    /// <code>
    /// Comparison&lt;MyDataRow&gt; comp = new Comparison&lt;MyDataRow&gt;(
    ///     (x, y) => y.Value2 - x.Value2
    /// );
    /// Sort&lt;MyDataRow&gt; block = new Sort&lt;MyDataRow&gt;(comp);
    /// </code>
    /// </example>
    public class Sort<TInput> : DataFlowTransformation<TInput, TInput>, ITask, IDataFlowTransformation<TInput, TInput>
    {
        #region Public properties

        public override string TaskName { get; set; } = "Sort";
        public Comparison<TInput> SortFunction { get; set; }
        //{
        //    get { return _sortFunction; }
        //    set
        //    {
        //        _sortFunction = value;
        //        InitBufferObjects();
        //    }
        //}

        public override ISourceBlock<TInput> SourceBlock => BlockTransformation.SourceBlock;
        public override ITargetBlock<TInput> TargetBlock => BlockTransformation.TargetBlock;

        #endregion

        #region Constructors

        public Sort()
        {
            BlockTransformation = new BlockTransformation<TInput, TInput>(SortByFunc);
        }

        public Sort(Comparison<TInput> sortFunction) : this()
        {
            SortFunction = sortFunction;
        }


        #endregion

        #region Implement abstract methods

        public override void InitBufferObjects()
        {
            BlockTransformation.CopyTaskProperties(this);
            if (MaxBufferSize > 0) BlockTransformation.MaxBufferSize = this.MaxBufferSize;
            BlockTransformation.InitBufferObjects();
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }


        protected override void CleanUpOnFaulted(Exception e) { }
        internal override void CompleteBufferOnPredecessorCompletion() => BlockTransformation.CompleteBufferOnPredecessorCompletion();

        internal override void FaultBufferOnPredecessorCompletion(Exception e) => BlockTransformation.FaultBufferOnPredecessorCompletion(e);



        #endregion

        #region Implementation

        BlockTransformation<TInput, TInput> BlockTransformation { get; set; }

        List<TInput> SortByFunc(List<TInput> data)
        {
            data.Sort(SortFunction);
            return data;
        }

        #endregion

    }

    /// <summary>
    /// Sort the input with the given sort function. The non generic implementation works with a dyanmic object.
    /// </summary>
    public class Sort : Sort<ExpandoObject>
    {
        public Sort() : base()
        { }

        public Sort(Comparison<ExpandoObject> sortFunction) : base(sortFunction)
        { }
    }


}
