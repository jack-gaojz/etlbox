using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// This transformation allow you to transform your input data into multple output data records.
    /// </summary>
    /// <typeparam name="TInput">Type of data input</typeparam>
    /// <typeparam name="TOutput">Type of data output</typeparam>
    public class RowMultiplication<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>, ILoggableTask, IDataFlowTransformation<TInput, TOutput>
    {
        #region Public properties

        public override string TaskName { get; set; } = $"Duplicate rows";
        public override ISourceBlock<TOutput> SourceBlock => TransformBlock;
        public override ITargetBlock<TInput> TargetBlock => TransformBlock;
        public Func<TInput, IEnumerable<TOutput>> MultiplicationFunc { get; set; }

        #endregion

        #region Constructors

        public RowMultiplication()
        {

        }

        public RowMultiplication(Func<TInput, IEnumerable<TOutput>> multiplicationFunc) : this()
        {
            MultiplicationFunc = multiplicationFunc;
        }

        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {
            TransformBlock = new TransformManyBlock<TInput, TOutput>(MultiplicateRow, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize,
            });
        }

        protected override void CleanUpOnSuccess() {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        #endregion

        #region Implementation

        TransformManyBlock<TInput, TOutput> TransformBlock { get; set; }

        private IEnumerable<TOutput> MultiplicateRow(TInput row)
        {
            NLogStartOnce();
            if (row == null) return null;
            try
            {
                return MultiplicationFunc?.Invoke(row);
            }
            catch (Exception e)
            {
                ThrowOrRedirectError(e, ErrorSource.ConvertErrorData<TInput>(row));
                return default;
            }
        }

        #endregion
    }

    /// <summary>
    /// This transformation allow you to transform your input data into multple output data records.
    /// </summary>
    /// <see cref="RowMultiplication{TInput, TOutput}"/>
    public class RowMultiplication : RowMultiplication<ExpandoObject, ExpandoObject>
    {
        public RowMultiplication() : base()
        { }

        public RowMultiplication(Func<ExpandoObject, IEnumerable<ExpandoObject>> multiplicationFunc)
            : base(multiplicationFunc)
        { }
    }

    /// <inheritdoc/>
    public class RowMultiplication<TInput> : RowMultiplication<TInput, TInput>
    {
        public RowMultiplication() : base()
        { }

        public RowMultiplication(Func<TInput, IEnumerable<TInput>> multiplicationFunc)
            : base(multiplicationFunc)
        { }
    }
}
