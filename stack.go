package sokv

type stackElement struct {
	node interface{}
	tag  uint64
}

type stack struct {
	list []stackElement
}

func (s *stack) push(e stackElement) {
	s.list = append(s.list, e)
}

func (s *stack) pop() stackElement {
	if len(s.list) == 0 {
		return stackElement{
			node: nil,
		}
	}
	v := s.list[len(s.list)-1]
	s.list = s.list[:len(s.list)-1]
	return v
}

func (s *stack) peek() stackElement {
	if len(s.list) == 0 {
		return stackElement{}
	} else {
		return s.list[len(s.list)-1]
	}
}
