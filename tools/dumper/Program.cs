using System;
using System.IO;
using System.Linq;

var d = CloudDriveSrv.Protos.CloudDriveReflection.Descriptor;
File.WriteAllBytes("/out/clouddrive.desc", d.SerializedData.ToArray());
Console.WriteLine($"name={d.Name} pkg={d.Package} msgs={d.MessageTypes.Count} services={d.Services.Count}");
foreach (var s in d.Services)
{
    Console.WriteLine($"service {s.FullName}");
    foreach (var m in s.Methods)
        Console.WriteLine($"  {m.Name} | in={m.InputType.FullName} | out={m.OutputType.FullName} | cs={m.IsClientStreaming} ss={m.IsServerStreaming}");
}
