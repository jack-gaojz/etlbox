﻿using ETLBox.ControlFlow;
using System;
using System.Dynamic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// Transforms the data row-by-row with the help of the transformation function.
    /// </summary>
    /// <typeparam name="TInput">Type of input data.</typeparam>
    /// <typeparam name="TOutput">Type of output data.</typeparam>
    /// <see cref="RowTransformation"/>
    /// <example>
    /// <code>
    /// RowTransformation&lt;string[], MyDataRow&gt; trans = new RowTransformation&lt;string[], MyDataRow&gt;(
    ///     csvdata => {
    ///       return new MyDataRow() { Value1 = csvdata[0], Value2 = int.Parse(csvdata[1]) };
    /// });
    /// trans.LinkTo(dest);
    /// </code>
    /// </example>
    public class RowTransformation<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>, ITask, IDataFlowTransformation<TInput, TOutput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Execute row transformation";

        /* Public Properties */
        public Func<TInput, TOutput> TransformationFunc { get; set; }
        public Action InitAction { get; set; }
        public bool WasInitialized { get; private set; } = false;


        public override ITargetBlock<TInput> TargetBlock => TransformBlock;
        public override ISourceBlock<TOutput> SourceBlock => TransformBlock;

        /* Private stuff */
        internal TransformBlock<TInput, TOutput> TransformBlock { get; set; }
        internal ErrorHandler ErrorHandler { get; set; } = new ErrorHandler();

        public RowTransformation()
        {
        }

        public RowTransformation(Func<TInput, TOutput> rowTransformationFunc) : this()
        {
            TransformationFunc = rowTransformationFunc;
        }


        public RowTransformation(Func<TInput, TOutput> rowTransformationFunc, Action initAction) : this(rowTransformationFunc)
        {
            this.InitAction = initAction;
        }

        internal RowTransformation(ITask task) : this()
        {
            CopyTaskProperties(task);
        }

        internal RowTransformation(ITask task, Func<TInput, TOutput> rowTransformationFunc) : this(rowTransformationFunc)
        {
            CopyTaskProperties(task);
        }

        protected override void InitBufferObjects()
        {
            TransformBlock = new TransformBlock<TInput, TOutput>(
                row =>
                {
                    try
                    {
                        return WrapTransformation(row);
                    }
                    catch (Exception e)
                    {
                        if (ErrorSource == null)
                        {
                            FaultPredecessorsRecursively(e);
                            throw e;
                        }
                        ErrorSource.Send(e, ErrorSource.ConvertErrorData<TInput>(row));
                        return default(TOutput);
                    }
                }, new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = MaxBufferSize,
                }
            );
        }

        protected override void CleanUpOnSuccess()
        {
            ErrorSource?.SourceBlock.Complete();
        }


        private TOutput WrapTransformation(TInput row)
        {
            if (!WasInitialized)
            {
                ErrorSource?.ExecuteAsync().Wait();
                InitAction?.Invoke();
                WasInitialized = true;
                if (!DisableLogging)
                    NLogger.Debug(TaskName + " was initialized!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
            }
            LogProgress();
            return TransformationFunc.Invoke(row);
        }
    }

    /// <summary>
    /// Transforms the data row-by-row with the help of the transformation function.
    /// </summary>
    /// <typeparam name="TInput">Type of input (and output) data.</typeparam>
    /// <see cref="RowTransformation{TInput, TOutput}"/>
    /// <example>
    /// <code>
    /// RowTransformation&lt;MyDataRow&gt; trans = new RowTransformation&lt;MyDataRow&gt;(
    ///     row => {
    ///       row.Value += 1;
    ///       return row;
    /// });
    /// trans.LinkTo(dest);
    /// </code>
    /// </example>
    public class RowTransformation<TInput> : RowTransformation<TInput, TInput>
    {
        public RowTransformation() : base() { }
        public RowTransformation(Func<TInput, TInput> rowTransformationFunc) : base(rowTransformationFunc) { }
        public RowTransformation(Func<TInput, TInput> rowTransformationFunc, Action initAction) : base(rowTransformationFunc, initAction) { }
    }

    /// <summary>
    /// Transforms the data row-by-row with the help of the transformation function.
    /// The non generic RowTransformation accepts a dynamic object as input and returns a dynamic object as output.
    /// If you need other data types, use the generic RowTransformation instead.
    /// </summary>
    /// <see cref="RowTransformation{TInput, TOutput}"/>
    /// <example>
    /// <code>
    /// //Non generic RowTransformation works with dynamic object as input and output
    /// //use RowTransformation&lt;TInput,TOutput&gt; for generic usage!
    /// RowTransformation trans = new RowTransformation(
    ///     csvdata => {
    ///       return new string[] { csvdata[0],  int.Parse(csvdata[1]) };
    /// });
    /// trans.LinkTo(dest);
    /// </code>
    /// </example>
    public class RowTransformation : RowTransformation<ExpandoObject>
    {
        public RowTransformation() : base() { }
        public RowTransformation(Func<ExpandoObject, ExpandoObject> rowTransformationFunc) : base(rowTransformationFunc) { }
        public RowTransformation(Func<ExpandoObject, ExpandoObject> rowTransformationFunc, Action initAction) : base(rowTransformationFunc, initAction) { }
    }
}
