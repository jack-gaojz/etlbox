using ETLBox.ControlFlow;
using System.Security.Cryptography;
using System.Text;


namespace ETLBox.Helper
{
    public static class HashHelper
    {
        public static string Encrypt_Char40(string text)
        {
            if (text != null)
            {
                string hex = "";
                byte[] hashValue = new SHA1Managed().ComputeHash(Encoding.UTF8.GetBytes(text));
                foreach (byte hashByte in hashValue)
                    hex += hashByte.ToString("x2");
                return hex.ToUpper();
            }
            else
                return "";
        }

        public static string Encrypt_Char40(ILoggableTask task) => Encrypt_Char40(task.TaskName + "|" + task.TaskType + "|" + task.GetHashCode());
        //public static string Encrypt_Char40(ILoggableTask task, string id) => Encrypt_Char40(task.TaskName + "|" + task.TaskType + "|" + id);
    }
}
