using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ETLBox.DataFlow
{
    public interface IDataFlowLogging
    {
        int ProgressCount { get; set; }
        int? LoggingThresholdRows { get; set; }
    }
}
