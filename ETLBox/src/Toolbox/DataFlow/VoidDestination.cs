using ETLBox.ControlFlow;
using NLog.Targets.Wrappers;
using System;
using System.Dynamic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// This destination if used as a trash.
    /// Redirect all data in this destination which you do not want for further processing.
    /// Every records needs to be transferred to a destination to have a dataflow completed.
    /// </summary>
    /// <typeparam name="TInput">Type of datasoure input.</typeparam>
    public class VoidDestination<TInput> : DataFlowDestination<TInput>
    {
        #region Public properties

        public override string TaskName => $"Void destination - Ignore data";
 
        #endregion

        #region Constructors

        public VoidDestination()
        {


        }

        #endregion

        #region Implement abstract interfaces

        public override void InitBufferObjects()
        {
            TargetAction = new ActionBlock<TInput>(r => { });
            WereBufferInitialized = true;
        }

        protected override void CleanUpOnSuccess()
        {
        }

        protected override void CleanUpOnFaulted(Exception e)
        {
        }
        #endregion

    }

    /// <summary>
    /// This destination if used as a trash.
    /// Redirect all data in this destination which you do not want for further processing.
    /// Every records needs to be transferred to a destination to have a dataflow completed.
    /// The non generic implementation works with a dynamic obect as input.
    /// </summary>
    public class VoidDestination : VoidDestination<ExpandoObject>
    {
        public VoidDestination() : base()
        { }
    }
}
