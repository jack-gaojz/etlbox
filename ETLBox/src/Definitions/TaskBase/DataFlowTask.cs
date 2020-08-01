using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowTask : GenericTask, ITask
    {
        public List<DataFlowTask> Predecessors { get; set; } = new List<DataFlowTask>();
        public List<DataFlowTask> Successors { get; set; } = new List<DataFlowTask>();

        public Task Completion { get; set; }

        protected int? _loggingThresholdRows;
        public virtual int? LoggingThresholdRows
        {
            get
            {
                if ((DataFlow.LoggingThresholdRows ?? 0) > 0)
                    return DataFlow.LoggingThresholdRows;
                else
                    return _loggingThresholdRows;
            }
            set
            {
                _loggingThresholdRows = value;
            }
        }

        public int ProgressCount { get; set; }

        protected bool HasLoggingThresholdRows => LoggingThresholdRows != null && LoggingThresholdRows > 0;
        protected int ThresholdCount { get; set; } = 1;

        public int MaxBufferSize
        {
            get
            {
                return _maxBufferSize > 0 ? _maxBufferSize : DataFlow.MaxBufferSize;
            }
            set
            {
                _maxBufferSize = value;
            }
        }

        protected int _maxBufferSize = -1;

        protected virtual void InitBufferObjects() { }

        protected bool WereBufferInitialized { get; set; }
        protected Dictionary<DataFlowTask, bool> WasLinked { get; set; } = new Dictionary<DataFlowTask, bool>();

        protected void InitBufferRecursively()
        {
            InitBufferObjects();
            //WereBufferInitialized = true;
            //foreach (DataFlowTask predecessor in Predecessors)
            //{
            //    if (!predecessor.WereBufferInitialized)
            //        predecessor.InitBufferRecursively();
            //}
            foreach (DataFlowTask successor in Successors)
            {
                //if (!successor.WereBufferInitialized)
                    successor.InitBufferRecursively();
            }

        }

        public DataFlowTask LinkTo2(DataFlowTask target)
        {
            this.Successors.Add(target);
            target.Predecessors.Add(this);
            return target;// as IDataFlowLinkSource<TOutput>;
        }

        protected void LinkBuffersRecursively()
        {
            //foreach (DataFlowTask predecessor in Predecessors)
            //{
            //    if (!predecessor.WasLinked.ContainsKey(this))
            //    {
            //        predecessor.LinkBuffers(this);
            //        predecessor.WasLinked.Add(this, true);
            //        predecessor.LinkBuffersRecursively();
            //    }
            //}
            foreach (DataFlowTask successor in Successors)
            {
                //if (!WasLinked.ContainsKey(successor))
                //{
                    LinkBuffers(successor);
                    //WasLinked.Add(successor, true);
                    successor.LinkBuffersRecursively();
                //}
            }
        }
        protected virtual void LinkBuffers(DataFlowTask succesor)
        {
            //TODO throw new exception that a destionation can't link or something
        }

        protected void SetCompletionTaskInDestinationsRecursively()
        {
            List<Task> CompletionTasks = new List<Task>();
            foreach (DataFlowTask pre in Predecessors)
            {
                CompletionTasks.Add(pre.BufferCompletion);
            }
            if (CompletionTasks.Count > 0)
                Completion = Task.WhenAll(CompletionTasks).ContinueWith(CompleteOrFaultBuffer, TaskContinuationOptions.ExecuteSynchronously);

            foreach (DataFlowTask succesor in Successors)
                succesor.SetCompletionTaskInDestinationsRecursively();
        }


        protected void FaultPredecessorsRecursively(Exception e)
        {
            FaultBuffer(e);
            foreach (DataFlowTask pre in Predecessors)
            {

                pre.FaultPredecessorsRecursively(e);
            }
        }


        protected virtual void FaultBuffer(Exception e)
        {

        }

        protected virtual void CompleteOrFaultBuffer(Task t)
        {

        }

        protected virtual Task BufferCompletion { get; }

        protected void NLogStart()
        {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

        protected void NLogFinish()
        {
            if (!DisableLogging && HasLoggingThresholdRows)
                NLogger.Info(TaskName + $" processed {ProgressCount} records in total.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

        protected void LogProgressBatch(int rowsProcessed)
        {
            ProgressCount += rowsProcessed;
            if (!DisableLogging && HasLoggingThresholdRows && ProgressCount >= (LoggingThresholdRows * ThresholdCount))
            {
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
                ThresholdCount++;
            }
        }

        protected void LogProgress()
        {
            ProgressCount += 1;
            if (!DisableLogging && HasLoggingThresholdRows && (ProgressCount % LoggingThresholdRows == 0))
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

    }

}
