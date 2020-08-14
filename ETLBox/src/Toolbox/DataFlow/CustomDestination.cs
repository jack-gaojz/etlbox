using ETLBox.ControlFlow;
using System;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// Define your own destination block.
    /// </summary>
    /// <typeparam name="TInput">Type of datasource input.</typeparam>
    public class CustomDestination<TInput> : DataFlowDestination<TInput>, ITask, IDataFlowDestination<TInput>
    {
        #region Public properties

        public override string TaskName { get; set; } = $"Write data into custom target";
        public Action<TInput> WriteAction { get; set; }

        #endregion

        #region Constructros

        public CustomDestination()
        {

        }

        public CustomDestination(Action<TInput> writeAction) : this()
        {
            WriteAction = writeAction;
        }

        #endregion

        #region Implement abstract methods

        public override void InitBufferObjects()
        {
            TargetAction = new ActionBlock<TInput>(AddLoggingAndErrorHandling(WriteAction), new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
            WereBufferInitialized = true;
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinish();
        }
        protected override void CleanUpOnFaulted(Exception e) { }

        #endregion

        #region Implementation

        private Action<TInput> AddLoggingAndErrorHandling(Action<TInput> writeAction)
        {
            return new Action<TInput>(
                input =>
                {
                    NLogStartOnce();
                    try
                    {
                        if (input != null)
                            writeAction.Invoke(input);
                    }
                    catch (Exception e)
                    {
                        ThrowOrRedirectError(e, ErrorSource.ConvertErrorData<TInput>(input));
                    }
                    LogProgress();
                });
        }

        #endregion
    }

    /// <summary>
    /// Define your own destination block. The non generic implementation uses a dynamic object as input.
    /// </summary>
    public class CustomDestination : CustomDestination<ExpandoObject>
    {
        public CustomDestination() : base()
        { }

        public CustomDestination(Action<ExpandoObject> writeAction) : base(writeAction)
        { }
    }
}
