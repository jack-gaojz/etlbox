﻿namespace ALE.ETLBox {
    /// <summary>
    /// Drop cube task will drop a cube database.
    /// </summary>
    public class DropCubeTask : GenericTask, ITask {
        public override string TaskType { get; set; } = "DROPCUBE";
        public override string TaskName => $"Drops cube {ASConnectionManager.ConnectionString.CatalogName}";
        public override void Execute() {
            NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            using (var conn = ASConnectionManager.Clone()) {
                conn.Open();
                conn.DropIfExists();
            }
            NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }
        public DropCubeTask() {
            NLogger = NLog.LogManager.GetLogger("ETL");
        }

        public DropCubeTask(string name) : this() {
            this.TaskName = name;
        }

        public static void Execute(string name) => new DropCubeTask(name).Execute();

        NLog.Logger NLogger { get; set; }

    }
}
