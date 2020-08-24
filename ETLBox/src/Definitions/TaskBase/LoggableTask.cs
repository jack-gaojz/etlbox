using ETLBox.Connection;
using ETLBox.Helper;
using System;

namespace ETLBox.ControlFlow
{
    public abstract class LoggableTask : ILoggableTask
    {
        private string _taskType;
        public virtual string TaskType
        {
            get => String.IsNullOrEmpty(_taskType) ? this.GetType().Name : _taskType;
            set => _taskType = value;
        }
        public virtual string TaskName { get; set; } = "N/A";
        public NLog.Logger NLogger { get; set; } = ControlFlow.GetLogger();

        public bool _disableLogging;
        public virtual bool DisableLogging
        {
            get
            {
                if (ControlFlow.DisableAllLogging == false)
                    return _disableLogging;
                else
                    return ControlFlow.DisableAllLogging;
            }
            set
            {
                _disableLogging = value;
            }
        }

        private string _taskHash;


        public virtual string TaskHash
        {
            get
            {
                if (_taskHash == null)
                    return HashHelper.Encrypt_Char40(this);
                else
                    return _taskHash;
            }
            set
            {
                _taskHash = value;
            }
        }
        internal virtual bool HasName => !String.IsNullOrWhiteSpace(TaskName);

        public LoggableTask()
        { }

        public void CopyLogTaskProperties(ILoggableTask otherTask)
        {
            this.TaskName = otherTask.TaskName;
            this.TaskHash = otherTask.TaskHash;
            this.TaskType = otherTask.TaskType;
            if (this.DisableLogging == false)
                this.DisableLogging = otherTask.DisableLogging;
        }
    }
}
