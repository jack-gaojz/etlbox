using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// A block transformation will wait for all data to be loaded into the buffer before the transformation is applied. After all data is in the buffer, the transformation
    /// is execution and the result posted into the targets.
    /// </summary>
    /// <typeparam name="TInput">Type of data input</typeparam>
    /// <typeparam name="TOutput">Type of data output</typeparam>
    /// <example>
    /// <code>
    /// BlockTransformation&lt;MyInputRow, MyOutputRow&gt; block = new BlockTransformation&lt;MyInputRow, MyOutputRow&gt;(
    ///     inputDataAsList => {
    ///         return inputData.Select( inputRow => new MyOutputRow() { Value2 = inputRow.Value1 }).ToList();
    ///     });
    /// block.LinkTo(dest);
    /// </code>
    /// </example>
    public class BlockTransformation<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>
    {
        #region Public properties

        public override string TaskName { get; set; } = "Excecute block transformation";
        public Func<List<TInput>, List<TOutput>> BlockTransformationFunc { get; set; }
        public override ISourceBlock<TOutput> SourceBlock => OutputBuffer;
        public override ITargetBlock<TInput> TargetBlock => InputBuffer;

        #endregion

        #region Constructors

        public BlockTransformation()
        {
            InputData = new List<TInput>();
        }

        public BlockTransformation(Func<List<TInput>, List<TOutput>> blockTransformationFunc) : this()
        {
            BlockTransformationFunc = blockTransformationFunc;
        }

        #endregion

        #region Implement abstract methods

        public override void InitBufferObjects()
        {
            OutputBuffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
            InputBuffer = new ActionBlock<TInput>(row => InputData.Add(row),
                new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = -1 //All data is always loaded!
                });
            InputBuffer.Completion.ContinueWith(t =>
            {
                if (t.IsFaulted) ((IDataflowBlock)OutputBuffer).Fault(t.Exception.InnerException);
                try
                {
                    NLogStartOnce();
                    WriteIntoOutput();
                    OutputBuffer.Complete();
                }
                catch (Exception e)
                {
                    ((IDataflowBlock)OutputBuffer).Fault(e);
                    throw e;
                }
            });
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }


        protected override void CleanUpOnFaulted(Exception e) { }

        internal override void CompleteBufferOnPredecessorCompletion() => TargetBlock.Complete();

        internal override void FaultBufferOnPredecessorCompletion(Exception e) => TargetBlock.Fault(e);


        #endregion

        #region Implementation

        BufferBlock<TOutput> OutputBuffer;
        ActionBlock<TInput> InputBuffer;
        List<TInput> InputData;
        List<TOutput> OutputData;

        private void WriteIntoOutput()
        {
            OutputData = BlockTransformationFunc(InputData);
            foreach (TOutput row in OutputData)
            {
                if (!OutputBuffer.SendAsync(row).Result)
                    throw Exception;
                LogProgress();
            }
        }

        #endregion
    }

    /// <summary>
    /// A block transformation will wait for all data to be loaded into the buffer before the transformation is applied. After all data is in the buffer, the transformation
    /// is execution and the result posted into the targets.
    /// </summary>
    /// <typeparam name="TInput">Type of data input (equal type of data output)</typeparam>
    /// <example>
    /// <code>
    /// BlockTransformation&lt;MyDataRow&gt; block = new BlockTransformation&lt;MyDataRow&gt;(
    ///     inputData => {
    ///         return inputData.Select( row => new MyDataRow() { Value1 = row.Value1, Value2 = 3 }).ToList();
    ///     });
    /// block.LinkTo(dest);
    /// </code>
    /// </example>
    public class BlockTransformation<TInput> : BlockTransformation<TInput, TInput>
    {
        public BlockTransformation() : base ()
        { }
        public BlockTransformation(Func<List<TInput>, List<TInput>> blockTransformationFunc) : base(blockTransformationFunc)
        { }

    }

    /// <summary>
    /// A block transformation will wait for all data to be loaded into the buffer before the transformation is applied. After all data is in the buffer, the transformation
    /// is execution and the result posted into the targets.
    /// The non generic implementation uses dynamic objects as input and output
    /// </summary>
    public class BlockTransformation : BlockTransformation<ExpandoObject>
    {
        public BlockTransformation() : base()
        { }

        public BlockTransformation(Func<List<ExpandoObject>, List<ExpandoObject>> blockTransformationFunc) : base(blockTransformationFunc)
        { }

    }

}
