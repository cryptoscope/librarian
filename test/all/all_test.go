package all

import (
	"testing"

	ltest "cryptoscope.co/go/librarian/test"
)

func Test(t *testing.T) {
	t.Run("SourceSetterIndex", ltest.RunSourceSetterIndexTests)
	t.Run("SeqSetterIndex", ltest.RunSeqSetterIndexTests)
	t.Run("SetterIndex", ltest.RunSetterIndexTests)
	t.Run("SinkIndex", ltest.RunSinkIndexTests)
}
