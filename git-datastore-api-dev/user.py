from pydantic import BaseModel


class Input(BaseModel):
    email: str = 'belyj69@gmail.com'
    name: str = 'belyj69'
    project: str = 'belyj69/finchproject'
    branch: str = '43202@python-gitlab'


class Input_file(Input):
    file_path: str = 'main.py'
    file_with_content: str = 'main.py'
    commit: str = 'initial commit'


class User(object):

    def __init__(self, gl, email, name, project, branch):
        self.email = email
        self.name = name
        self.project = gl.projects.get(project)
        self.branch = branch

    def created_branch(self, branch):
        return self.project.branches.create({'branch': branch, 'ref': 'main'})

    def uploaded_file(self, file_path, branch, file_with_content, email, name, commit_message):
        return self.project.files.create({'file_path': file_path,
                                          'branch': branch,
                                          'content': open(file_with_content).read(),
                                          'author_email': email,
                                          'author_name': name,
                                          'commit_message': commit_message})

    def get_file(self, file_path, branch):
        return self.project.files.get(file_path=file_path, ref=branch)

    def update_file(self, file_path, branch, content, commit_message):
        f = self.get_file(file_path, branch)
        f.content = content
        f.save(branch=branch, commit_message=commit_message)

    def created_commit(self, branch, commit_message, file_path, file_with_content):
        data = {
            'branch': branch,
            'commit_message': commit_message,
            'actions': [
                {
                    'action': 'create',
                    'file_path': file_path,
                    'content': open(file_with_content).read(),
                }
            ]
        }

        return self.project.commits.create(data)
