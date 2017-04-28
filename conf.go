package gsnake

type Conf struct {
	StatusFile  string
	FilePattern string
	DirPattern  string

	owner *Dispatcher
}
