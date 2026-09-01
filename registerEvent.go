package Redelay

//// RegisterEvent Deprecated : use Publish instead
//func (s *Service) RegisterEvent(name string, args string, delay int64) error {
//	return s.Publish(name, args, delay)
//}
//
//func (s *Service) RegisterEventFunc(name string, f func(p string) error) {
//	s.mx.RLock()
//	_, ok := s.slot[name]
//	s.mx.RUnlock()
//	if ok {
//		panic("event already exists")
//	}
//
//	s.mx.Lock()
//	s.slot[name] = f
//	s.mx.Unlock()
//
//}
