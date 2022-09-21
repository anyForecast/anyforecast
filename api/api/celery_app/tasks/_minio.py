from ._base_task import BaseTask
from ..dataloaders import S3LoadersFactory


class ShowParquetPartitions(BaseTask):

    def __init__(self, serializer=None):
        super().__init__(serializer)

    def run(self, user, dataset):
        user, dataset = map(dict, (user, dataset))
        loader = S3LoadersFactory(user, dataset).get_loader('objects')

        objects = loader.load('parquet')
        partition_names = []
        for obj in objects:
            obj_name = obj.object_name
            if obj.is_dir:
                obj_name = obj_name[:-1]

            obj_name = obj_name.split('/')[-1]
            obj_name = obj_name.split('=')[-1]
            partition_names.append(obj_name)

        return partition_names
