package mesoslib

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/VoltFramework/volt/mesosproto"
	"github.com/golang/protobuf/proto"
)

type Volume struct {
	ContainerPath string `json:"container_path,omitempty"`
	HostPath      string `json:"host_path,omitempty"`
	Mode          string `json:"mode,omitempty"`
}

type Task struct {
	ID      string
	Command []string
	Image   string
	Volumes []*Volume
}

func createTaskInfo(offer *mesosproto.Offer, resources []*mesosproto.Resource, task *Task) *mesosproto.TaskInfo {
	taskInfo := mesosproto.TaskInfo{
		Name: proto.String(fmt.Sprintf("volt-task-%s", task.ID)),
		TaskId: &mesosproto.TaskID{
			Value: &task.ID,
		},
		SlaveId:   offer.SlaveId,
		Resources: resources,
		Command:   &mesosproto.CommandInfo{},
	}

	fmt.Printf("Task Info   %v\n", taskInfo);

	// Set value only if provided
	if task.Command[0] != "" {
		taskInfo.Command.Value = &task.Command[0]
	}

	// Set args only if they exist
	if len(task.Command) > 1 {
		taskInfo.Command.Arguments = task.Command[1:]
	}

	var str = new (string);
	*str = "-e";
	//taskInfo.Command.Value = str;
	var b = new(bool);
	*b = true;
	taskInfo.Command.Shell = b;
	//taskInfo.Command.Arguments = []string{"-e SWIFT_PWORKERS=3"}
	fmt.Printf("arguments  \n", taskInfo.Command);

	// Set the docker image if specified
	if task.Image != "" {
		taskInfo.Container = &mesosproto.ContainerInfo{
			Type: mesosproto.ContainerInfo_DOCKER.Enum(),
			Docker: &mesosproto.ContainerInfo_DockerInfo{
				Image: &task.Image,
			},
		}

		for _, v := range task.Volumes {
			var (
				vv   = v
				mode = mesosproto.Volume_RW
			)

			if vv.Mode == "ro" {
				mode = mesosproto.Volume_RO
			}

			taskInfo.Container.Volumes = append(taskInfo.Container.Volumes, &mesosproto.Volume{
				ContainerPath: &vv.ContainerPath,
				HostPath:      &vv.HostPath,
				Mode:          &mode,
			})
		}

		taskInfo.Command.Shell = proto.Bool(true)
		
		var params = &mesos.proto.Parameter{ 
							Key:"-e",
							Value:"SWIFT_PWORKERS=3", 
						  }

//		taskInfo.Container.Docker.Parameters := []*mesos.proto.Parameters{params};
		fmt.Printf("\n\n");
		fmt.Printf("Docker  %v\n", taskInfo.Container.Docker.Parameters); 		
		fmt.Printf("Task Info Command  %v\n", taskInfo.Command);
		fmt.Printf("\n\n");
	}

	return &taskInfo
}

func (m *MesosLib) LaunchTask(offer *mesosproto.Offer, resources []*mesosproto.Resource, task *Task) error {
	m.Log.WithFields(logrus.Fields{"ID": task.ID, "command_arjun": task.Command, "offerId": offer.Id, "dockerImage": task.Image}).Info("Launching task...")

	//fmt.Printf("Task  %v\n", task);
	fmt.Printf("Command  %v\n", task.Command);

        for i:= range resources {
		fmt.Printf("%v  i= %v\n", resources[i], i);
	}

	taskInfo := createTaskInfo(offer, resources, task)

	fmt.Printf("\nCommandInfo_ContainerInfo   %v\n", taskInfo.Command.Container);

	return m.send(&mesosproto.LaunchTasksMessage{
		FrameworkId: m.frameworkInfo.Id,
		Tasks:       []*mesosproto.TaskInfo{taskInfo},
		OfferIds: []*mesosproto.OfferID{
			offer.Id,
		},
		Filters: &mesosproto.Filters{},
	}, "mesos.internal.LaunchTasksMessage")
}

func (m *MesosLib) KillTask(ID string) error {
	m.Log.WithFields(logrus.Fields{"ID": ID}).Info("Killing task...")

	return m.send(&mesosproto.KillTaskMessage{
		FrameworkId: m.frameworkInfo.Id,
		TaskId: &mesosproto.TaskID{
			Value: &ID,
		},
	}, "mesos.internal.KillTaskMessage")
}
