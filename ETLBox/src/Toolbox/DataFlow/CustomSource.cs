using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using System;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// Define your own source block.
    /// </summary>
    /// <typeparam name="TOutput">Type of data output.</typeparam>
    public class CustomSource<TOutput> : DataFlowExecutableSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        #region Public properties

        public override string TaskName => $"Read data from custom source";
        public Func<TOutput> ReadFunc { get; set; }
        public Func<bool> ReadCompletedFunc { get; set; }

        #endregion

        #region Constructors

        public CustomSource()
        {
        }

        public CustomSource(Func<TOutput> readFunc, Func<bool> readCompletedFunc) : this()
        {
            ReadFunc = readFunc;
            ReadCompletedFunc = readCompletedFunc;
        }

        #endregion

        #region Implement abstract methods

        protected override void OnExecutionDoSynchronousWork() { }

        protected override void OnExecutionDoAsyncWork()
        {
            NLogStartOnce();
            ReadAllRecords();
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        #endregion

        #region Implementation

        private void ReadAllRecords()
        {
            while (!ReadCompletedFunc.Invoke())
            {
                TOutput result = default;
                try
                {
                    result = ReadFunc.Invoke();
                    if (!Buffer.SendAsync(result).Result)
                        throw new ETLBoxException("Buffer already completed or faulted!", this.Exception);
                }
                catch (ETLBoxException) { throw; }
                catch (Exception e)
                {
                    ThrowOrRedirectError(e, e.Message);
                }

                LogProgress();
            }
        }

        #endregion

    }

    /// <summary>
    /// Define your own source block. The non generic implementation returns a dynamic object as output.
    /// </summary>
    public class CustomSource : CustomSource<ExpandoObject>
    {
        public CustomSource() : base()
        { }

        public CustomSource(Func<ExpandoObject> readFunc, Func<bool> readCompletedFunc) : base(readFunc, readCompletedFunc)
        { }
    }
}
